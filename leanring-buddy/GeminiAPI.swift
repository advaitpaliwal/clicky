//
//  GeminiAPI.swift
//  leanring-buddy
//
//  Gemini (Google Generative Language) vision + chat client with SSE
//  streaming. Used in place of the Claude/Cloudflare path for local
//  prototyping — calls the API directly with a locally-stored key.
//
//  Mirrors ClaudeAPI's public surface (analyzeImageStreaming / analyzeImage)
//  so it is a drop-in replacement for CompanionManager.
//

import Foundation

class GeminiAPI {
    private let apiKey: String

    /// The model token as set by the app's picker. May still be a Claude id
    /// (e.g. "claude-sonnet-4-6") — it is mapped to a Gemini model at request time.
    var model: String

    private let session: URLSession

    init(apiKey: String, model: String = "gemini-3.5-flash") {
        self.apiKey = apiKey
        self.model = model

        let configuration = URLSessionConfiguration.default
        configuration.timeoutIntervalForRequest = 120
        configuration.timeoutIntervalForResource = 300
        configuration.waitsForConnectivity = true
        configuration.urlCache = nil
        configuration.httpCookieStorage = nil
        self.session = URLSession(configuration: configuration)
    }

    /// Maps the stored model token onto a concrete Gemini model. The picker may
    /// still hand us a Claude id, so "opus"/"pro" select the stronger model and
    /// everything else uses the fast flash model.
    private func resolveGeminiModelName() -> String {
        let lowercasedModel = model.lowercased()
        if lowercasedModel.contains("gemini") {
            return model
        }
        if lowercasedModel.contains("opus") || lowercasedModel.contains("pro") {
            return "gemini-3.1-pro-preview"
        }
        return "gemini-3.5-flash"
    }

    /// Detects the MIME type of image data by inspecting the first bytes.
    /// ScreenCaptureKit captures are JPEG; pasted clipboard images are PNG.
    private func detectImageMimeType(for imageData: Data) -> String {
        if imageData.count >= 4 {
            let pngSignature: [UInt8] = [0x89, 0x50, 0x4E, 0x47]
            if [UInt8](imageData.prefix(4)) == pngSignature {
                return "image/png"
            }
        }
        return "image/jpeg"
    }

    private func makeStreamingRequest() throws -> URLRequest {
        let geminiModelName = resolveGeminiModelName()
        let endpointURLString =
            "https://generativelanguage.googleapis.com/v1beta/models/\(geminiModelName):streamGenerateContent?alt=sse&key=\(apiKey)"

        guard let endpointURL = URL(string: endpointURLString) else {
            throw NSError(
                domain: "GeminiAPI",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Invalid Gemini endpoint URL"]
            )
        }

        var request = URLRequest(url: endpointURL)
        request.httpMethod = "POST"
        request.timeoutInterval = 120
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        return request
    }

    /// Builds the Gemini request body. Conversation history maps user/assistant
    /// turns onto Gemini's user/model roles; the current turn carries all labeled
    /// screenshots (as inline_data parts) followed by the user's prompt.
    private func buildRequestBody(
        images: [(data: Data, label: String)],
        systemPrompt: String,
        conversationHistory: [(userPlaceholder: String, assistantResponse: String)],
        userPrompt: String,
        maxOutputTokens: Int
    ) -> [String: Any] {
        var contents: [[String: Any]] = []

        for (userPlaceholder, assistantResponse) in conversationHistory {
            contents.append(["role": "user", "parts": [["text": userPlaceholder]]])
            contents.append(["role": "model", "parts": [["text": assistantResponse]]])
        }

        var currentTurnParts: [[String: Any]] = []
        for image in images {
            currentTurnParts.append([
                "inline_data": [
                    "mime_type": detectImageMimeType(for: image.data),
                    "data": image.data.base64EncodedString()
                ]
            ])
            currentTurnParts.append(["text": image.label])
        }
        currentTurnParts.append(["text": userPrompt])
        contents.append(["role": "user", "parts": currentTurnParts])

        return [
            "system_instruction": ["parts": [["text": systemPrompt]]],
            "contents": contents,
            "generationConfig": [
                "maxOutputTokens": maxOutputTokens,
                // Keep thinking minimal so the first token arrives fast —
                // important for a real-time voice loop. (Gemini 3 uses thinkingLevel.)
                "thinkingConfig": ["thinkingLevel": "low"]
            ]
        ]
    }

    /// Send a vision request to Gemini with SSE streaming.
    /// Calls `onTextChunk` on the main actor with the accumulated text so far.
    func analyzeImageStreaming(
        images: [(data: Data, label: String)],
        systemPrompt: String,
        conversationHistory: [(userPlaceholder: String, assistantResponse: String)] = [],
        userPrompt: String,
        onTextChunk: @MainActor @Sendable (String) -> Void
    ) async throws -> (text: String, duration: TimeInterval) {
        let startTime = Date()

        var request = try makeStreamingRequest()
        let body = buildRequestBody(
            images: images,
            systemPrompt: systemPrompt,
            conversationHistory: conversationHistory,
            userPrompt: userPrompt,
            maxOutputTokens: 1024
        )
        let bodyData = try JSONSerialization.data(withJSONObject: body)
        request.httpBody = bodyData
        let payloadMB = Double(bodyData.count) / 1_048_576.0
        print("🌐 Gemini streaming request: \(String(format: "%.1f", payloadMB))MB, \(images.count) image(s)")

        let (byteStream, response) = try await session.bytes(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            throw NSError(
                domain: "GeminiAPI",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Invalid HTTP response"]
            )
        }

        guard (200...299).contains(httpResponse.statusCode) else {
            var errorBodyChunks: [String] = []
            for try await line in byteStream.lines {
                errorBodyChunks.append(line)
            }
            let errorBody = errorBodyChunks.joined(separator: "\n")
            throw NSError(
                domain: "GeminiAPI",
                code: httpResponse.statusCode,
                userInfo: [NSLocalizedDescriptionKey: "API Error (\(httpResponse.statusCode)): \(errorBody)"]
            )
        }

        // Parse the SSE stream — each event is "data: {GenerateContentResponse json}".
        var accumulatedResponseText = ""

        for try await line in byteStream.lines {
            guard line.hasPrefix("data: ") else { continue }
            let jsonString = String(line.dropFirst(6))

            guard jsonString != "[DONE]" else { break }

            guard let jsonData = jsonString.data(using: .utf8),
                  let eventPayload = try? JSONSerialization.jsonObject(with: jsonData) as? [String: Any] else {
                continue
            }

            let textChunk = Self.extractTextChunk(from: eventPayload)
            if !textChunk.isEmpty {
                accumulatedResponseText += textChunk
                let currentAccumulatedText = accumulatedResponseText
                await onTextChunk(currentAccumulatedText)
            }
        }

        let duration = Date().timeIntervalSince(startTime)
        return (text: accumulatedResponseText, duration: duration)
    }

    /// Non-streaming convenience that accumulates the streamed response.
    func analyzeImage(
        images: [(data: Data, label: String)],
        systemPrompt: String,
        conversationHistory: [(userPlaceholder: String, assistantResponse: String)] = [],
        userPrompt: String
    ) async throws -> (text: String, duration: TimeInterval) {
        return try await analyzeImageStreaming(
            images: images,
            systemPrompt: systemPrompt,
            conversationHistory: conversationHistory,
            userPrompt: userPrompt,
            onTextChunk: { _ in }
        )
    }

    /// Pulls the text out of a single Gemini SSE event payload.
    private static func extractTextChunk(from eventPayload: [String: Any]) -> String {
        guard let candidates = eventPayload["candidates"] as? [[String: Any]],
              let firstCandidate = candidates.first,
              let content = firstCandidate["content"] as? [String: Any],
              let parts = content["parts"] as? [[String: Any]] else {
            return ""
        }

        var combinedText = ""
        for part in parts {
            if let text = part["text"] as? String {
                combinedText += text
            }
        }
        return combinedText
    }
}
