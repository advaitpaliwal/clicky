//
//  DeepgramStreamingTranscriptionProvider.swift
//  leanring-buddy
//
//  Streaming transcription backed by Deepgram's real-time websocket API.
//  Drop-in replacement for the AssemblyAI provider used during local
//  prototyping — connects directly with a locally-stored key.
//

import AVFoundation
import Foundation

struct DeepgramStreamingTranscriptionProviderError: LocalizedError {
    let message: String
    var errorDescription: String? { message }
}

final class DeepgramStreamingTranscriptionProvider: BuddyTranscriptionProvider {
    let displayName = "Deepgram"
    let requiresSpeechRecognitionPermission = false

    var isConfigured: Bool { LocalSecrets.deepgramAPIKey != nil }
    var unavailableExplanation: String? {
        isConfigured
            ? nil
            : "Deepgram API key not found. Add \"deepgramAPIKey\" to ~/Library/Application Support/Clicky/Secrets.json"
    }

    /// Single long-lived URLSession shared across all streaming sessions.
    /// Creating and invalidating a URLSession per session corrupts the OS
    /// connection pool and causes "Socket is not connected" errors after a
    /// few rapid reconnections to the same host.
    private let sharedWebSocketURLSession = URLSession(configuration: .default)

    func startStreamingSession(
        keyterms: [String],
        onTranscriptUpdate: @escaping (String) -> Void,
        onFinalTranscriptReady: @escaping (String) -> Void,
        onError: @escaping (Error) -> Void
    ) async throws -> any BuddyStreamingTranscriptionSession {
        guard let apiKey = LocalSecrets.deepgramAPIKey else {
            throw DeepgramStreamingTranscriptionProviderError(message: "Deepgram API key not configured.")
        }

        let session = DeepgramStreamingTranscriptionSession(
            apiKey: apiKey,
            urlSession: sharedWebSocketURLSession,
            keyterms: keyterms,
            onTranscriptUpdate: onTranscriptUpdate,
            onFinalTranscriptReady: onFinalTranscriptReady,
            onError: onError
        )

        try await session.open()
        return session
    }
}

private final class DeepgramStreamingTranscriptionSession: NSObject, BuddyStreamingTranscriptionSession {
    /// Subset of Deepgram's real-time "Results" message we care about.
    private struct ResultsMessage: Decodable {
        struct Channel: Decodable {
            struct Alternative: Decodable { let transcript: String? }
            let alternatives: [Alternative]?
        }
        let type: String?
        let is_final: Bool?
        let speech_final: Bool?
        let channel: Channel?
    }

    private static let targetSampleRate = 16_000.0
    private static let explicitFinalTranscriptGracePeriodSeconds = 1.2

    let finalTranscriptFallbackDelaySeconds: TimeInterval = 2.8

    private let apiKey: String
    private let keyterms: [String]
    private let onTranscriptUpdate: (String) -> Void
    private let onFinalTranscriptReady: (String) -> Void
    private let onError: (Error) -> Void

    private let stateQueue = DispatchQueue(label: "com.clicky.deepgram.state")
    private let sendQueue = DispatchQueue(label: "com.clicky.deepgram.send")
    private let audioPCM16Converter = BuddyPCM16AudioConverter(targetSampleRate: targetSampleRate)
    private let urlSession: URLSession

    private var webSocketTask: URLSessionWebSocketTask?
    private var hasDeliveredFinalTranscript = false
    private var isAwaitingExplicitFinalTranscript = false
    /// Finalized transcript segments, in arrival order. Deepgram emits a fresh
    /// segment each time `is_final` is set, so we append rather than replace.
    private var committedTranscriptSegments: [String] = []
    /// The latest in-progress (interim) segment that hasn't been finalized yet.
    private var interimTranscriptText = ""
    private var explicitFinalTranscriptDeadlineWorkItem: DispatchWorkItem?

    init(
        apiKey: String,
        urlSession: URLSession,
        keyterms: [String],
        onTranscriptUpdate: @escaping (String) -> Void,
        onFinalTranscriptReady: @escaping (String) -> Void,
        onError: @escaping (Error) -> Void
    ) {
        self.apiKey = apiKey
        self.urlSession = urlSession
        self.keyterms = keyterms
        self.onTranscriptUpdate = onTranscriptUpdate
        self.onFinalTranscriptReady = onFinalTranscriptReady
        self.onError = onError
    }

    /// Opens the websocket. URLSessionWebSocketTask queues sends until the
    /// TLS/WS handshake completes and buffers receives, so there is nothing to
    /// await — we resume, start the receive loop, and return. Connection
    /// failures surface through the receive loop → failSession → onError.
    ///
    /// NOTE: we deliberately do NOT set a per-task `URLSessionWebSocketDelegate`.
    /// Doing so on this shared, configuration-only URLSession caused the socket
    /// to drop with "Socket is not connected" immediately after the handshake.
    /// The simpler resume-and-stream pattern is what works reliably.
    func open() async throws {
        let websocketURL = try Self.makeWebsocketURL(keyterms: keyterms)

        var websocketRequest = URLRequest(url: websocketURL)
        websocketRequest.setValue("Token \(apiKey)", forHTTPHeaderField: "Authorization")

        let webSocketTask = urlSession.webSocketTask(with: websocketRequest)
        self.webSocketTask = webSocketTask
        webSocketTask.resume()

        receiveNextMessage()
    }

    // MARK: BuddyStreamingTranscriptionSession

    func appendAudioBuffer(_ audioBuffer: AVAudioPCMBuffer) {
        guard let audioPCM16Data = audioPCM16Converter.convertToPCM16Data(from: audioBuffer),
              !audioPCM16Data.isEmpty else {
            return
        }

        sendQueue.async { [weak self] in
            guard let self, let webSocketTask = self.webSocketTask else { return }
            webSocketTask.send(.data(audioPCM16Data)) { [weak self] error in
                if let error {
                    self?.failSession(with: error)
                }
            }
        }
    }

    func requestFinalTranscript() {
        stateQueue.async {
            guard !self.hasDeliveredFinalTranscript else { return }
            self.isAwaitingExplicitFinalTranscript = true
            self.scheduleExplicitFinalTranscriptDeadline()
        }

        // Ask Deepgram to flush buffered audio into a final result immediately.
        sendJSONMessage(["type": "Finalize"])
    }

    func cancel() {
        stateQueue.async {
            self.explicitFinalTranscriptDeadlineWorkItem?.cancel()
            self.explicitFinalTranscriptDeadlineWorkItem = nil
        }

        sendJSONMessage(["type": "CloseStream"])
        webSocketTask?.cancel(with: .goingAway, reason: nil)
    }

    // MARK: Receive loop

    private func receiveNextMessage() {
        webSocketTask?.receive { [weak self] result in
            guard let self else { return }

            switch result {
            case .success(let message):
                switch message {
                case .string(let text):
                    self.handleIncomingTextMessage(text)
                case .data(let data):
                    if let text = String(data: data, encoding: .utf8) {
                        self.handleIncomingTextMessage(text)
                    }
                @unknown default:
                    break
                }

                self.receiveNextMessage()
            case .failure(let error):
                self.failSession(with: error)
            }
        }
    }

    private func handleIncomingTextMessage(_ text: String) {
        guard let messageData = text.data(using: .utf8),
              let resultsMessage = try? JSONDecoder().decode(ResultsMessage.self, from: messageData) else {
            return
        }

        guard (resultsMessage.type ?? "") == "Results" else { return }

        let transcriptText = resultsMessage.channel?.alternatives?.first?.transcript?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let isFinalSegment = resultsMessage.is_final ?? false
        let isSpeechFinal = resultsMessage.speech_final ?? false

        stateQueue.async {
            if isFinalSegment {
                if !transcriptText.isEmpty {
                    self.committedTranscriptSegments.append(transcriptText)
                }
                self.interimTranscriptText = ""
            } else {
                self.interimTranscriptText = transcriptText
            }

            let fullTranscriptText = self.composeFullTranscript()
            if !fullTranscriptText.isEmpty {
                self.onTranscriptUpdate(fullTranscriptText)
            }

            guard self.isAwaitingExplicitFinalTranscript else { return }

            // Once Deepgram signals end-of-speech (or returns the flushed final),
            // deliver the best transcript we have.
            if isSpeechFinal || (isFinalSegment && !transcriptText.isEmpty) {
                self.explicitFinalTranscriptDeadlineWorkItem?.cancel()
                self.explicitFinalTranscriptDeadlineWorkItem = nil
                self.deliverFinalTranscriptIfNeeded(self.bestAvailableTranscriptText())
            }
        }
    }

    private func composeFullTranscript() -> String {
        var transcriptSegments = committedTranscriptSegments

        let trimmedInterimText = interimTranscriptText.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmedInterimText.isEmpty {
            transcriptSegments.append(trimmedInterimText)
        }

        return transcriptSegments.joined(separator: " ")
    }

    private func bestAvailableTranscriptText() -> String {
        composeFullTranscript().trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func scheduleExplicitFinalTranscriptDeadline() {
        explicitFinalTranscriptDeadlineWorkItem?.cancel()

        let deadlineWorkItem = DispatchWorkItem { [weak self] in
            self?.stateQueue.async {
                guard let self else { return }
                self.deliverFinalTranscriptIfNeeded(self.bestAvailableTranscriptText())
            }
        }

        explicitFinalTranscriptDeadlineWorkItem = deadlineWorkItem

        DispatchQueue.main.asyncAfter(
            deadline: .now() + Self.explicitFinalTranscriptGracePeriodSeconds,
            execute: deadlineWorkItem
        )
    }

    private func deliverFinalTranscriptIfNeeded(_ transcriptText: String) {
        guard !hasDeliveredFinalTranscript else { return }
        hasDeliveredFinalTranscript = true
        explicitFinalTranscriptDeadlineWorkItem?.cancel()
        explicitFinalTranscriptDeadlineWorkItem = nil
        onFinalTranscriptReady(transcriptText)
        sendJSONMessage(["type": "CloseStream"])
    }

    private func sendJSONMessage(_ payload: [String: Any]) {
        guard let jsonData = try? JSONSerialization.data(withJSONObject: payload),
              let jsonString = String(data: jsonData, encoding: .utf8) else {
            return
        }

        sendQueue.async { [weak self] in
            guard let self, let webSocketTask = self.webSocketTask else { return }
            webSocketTask.send(.string(jsonString)) { [weak self] error in
                if let error {
                    self?.failSession(with: error)
                }
            }
        }
    }

    private func failSession(with error: Error) {
        stateQueue.async {
            let latestTranscriptText = self.bestAvailableTranscriptText()

            if self.isAwaitingExplicitFinalTranscript
                && !self.hasDeliveredFinalTranscript
                && !latestTranscriptText.isEmpty {
                print("[Deepgram] ⚠️ WebSocket error during active session, delivering partial transcript: \(error.localizedDescription)")
                self.deliverFinalTranscriptIfNeeded(latestTranscriptText)
                return
            }

            print("[Deepgram] ❌ Session failed with error: \(error.localizedDescription)")
            self.onError(error)
        }
    }

    private static func makeWebsocketURL(keyterms: [String]) throws -> URL {
        guard var websocketURLComponents = URLComponents(string: "wss://api.deepgram.com/v1/listen") else {
            throw DeepgramStreamingTranscriptionProviderError(message: "Deepgram websocket URL is invalid.")
        }

        var queryItems = [
            URLQueryItem(name: "model", value: "nova-3"),
            URLQueryItem(name: "encoding", value: "linear16"),
            URLQueryItem(name: "sample_rate", value: "16000"),
            URLQueryItem(name: "channels", value: "1"),
            URLQueryItem(name: "interim_results", value: "true"),
            URLQueryItem(name: "smart_format", value: "true"),
            URLQueryItem(name: "punctuate", value: "true")
        ]

        let normalizedKeyterms = keyterms
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
        for keyterm in normalizedKeyterms {
            queryItems.append(URLQueryItem(name: "keyterm", value: keyterm))
        }

        websocketURLComponents.queryItems = queryItems

        guard let websocketURL = websocketURLComponents.url else {
            throw DeepgramStreamingTranscriptionProviderError(message: "Deepgram websocket URL could not be created.")
        }

        return websocketURL
    }
}
