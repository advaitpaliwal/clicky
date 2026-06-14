//
//  DeepgramTTSClient.swift
//  leanring-buddy
//
//  Text-to-speech via Deepgram Aura. Drop-in replacement for the
//  ElevenLabs/Cloudflare path used during local prototyping — calls
//  Deepgram directly with a locally-stored key.
//

import AVFoundation
import Foundation

@MainActor
final class DeepgramTTSClient {
    private let apiKey: String
    private let voiceModel: String
    private let session: URLSession

    /// The audio player for the current TTS playback. Kept alive so the audio
    /// finishes playing even if the caller doesn't hold a reference.
    private var audioPlayer: AVAudioPlayer?

    init(apiKey: String, voiceModel: String = "aura-2-thalia-en") {
        self.apiKey = apiKey
        self.voiceModel = voiceModel

        let configuration = URLSessionConfiguration.default
        configuration.timeoutIntervalForRequest = 30
        configuration.timeoutIntervalForResource = 60
        self.session = URLSession(configuration: configuration)
    }

    /// Sends `text` to Deepgram Aura and plays the resulting audio.
    /// Throws on network or decoding errors. Cancellation-safe.
    func speakText(_ text: String) async throws {
        let endpointURLString = "https://api.deepgram.com/v1/speak?model=\(voiceModel)&encoding=mp3"
        guard let endpointURL = URL(string: endpointURLString) else {
            throw NSError(domain: "DeepgramTTS", code: -1,
                          userInfo: [NSLocalizedDescriptionKey: "Invalid Deepgram TTS URL"])
        }

        var request = URLRequest(url: endpointURL)
        request.httpMethod = "POST"
        request.setValue("Token \(apiKey)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("audio/mpeg", forHTTPHeaderField: "Accept")
        request.httpBody = try JSONSerialization.data(withJSONObject: ["text": text])

        let (data, response) = try await session.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            throw NSError(domain: "DeepgramTTS", code: -1,
                          userInfo: [NSLocalizedDescriptionKey: "Invalid response"])
        }

        guard (200...299).contains(httpResponse.statusCode) else {
            let errorBody = String(data: data, encoding: .utf8) ?? "Unknown error"
            throw NSError(domain: "DeepgramTTS", code: httpResponse.statusCode,
                          userInfo: [NSLocalizedDescriptionKey: "TTS API error (\(httpResponse.statusCode)): \(errorBody)"])
        }

        try Task.checkCancellation()

        let player = try AVAudioPlayer(data: data)
        self.audioPlayer = player
        player.play()
        print("🔊 Deepgram TTS: playing \(data.count / 1024)KB audio")
    }

    /// Whether TTS audio is currently playing back.
    var isPlaying: Bool {
        audioPlayer?.isPlaying ?? false
    }

    /// Stops any in-progress playback immediately.
    func stopPlayback() {
        audioPlayer?.stop()
        audioPlayer = nil
    }
}
