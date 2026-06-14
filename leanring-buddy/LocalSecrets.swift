//
//  LocalSecrets.swift
//  leanring-buddy
//
//  Loads API keys for local development so Clicky can call providers
//  directly without the Cloudflare Worker proxy. Keys are read from the
//  process environment first (handy for Xcode scheme env vars), then from a
//  JSON file that lives OUTSIDE the repo so keys are never committed:
//
//      ~/Library/Application Support/Clicky/Secrets.json
//
//  Format:
//      { "geminiAPIKey": "...", "deepgramAPIKey": "..." }
//

import Foundation

enum LocalSecrets {
    /// Loaded once at first access. The file is optional — if it's missing,
    /// callers fall back to environment variables (or get nil).
    private static let loadedSecretsFile: [String: String] = readSecretsFile()

    /// Gemini (Google Generative Language) API key for the LLM + vision calls.
    static var geminiAPIKey: String? {
        resolveValue(fileKey: "geminiAPIKey", environmentKeys: ["GEMINI_API_KEY", "GOOGLE_API_KEY"])
    }

    /// Deepgram API key, used for both streaming transcription and TTS.
    static var deepgramAPIKey: String? {
        resolveValue(fileKey: "deepgramAPIKey", environmentKeys: ["DEEPGRAM_API_KEY"])
    }

    /// The on-disk location Clicky reads keys from for local development.
    static var secretsFileURL: URL {
        let applicationSupportDirectory = FileManager.default.urls(
            for: .applicationSupportDirectory,
            in: .userDomainMask
        ).first ?? URL(fileURLWithPath: NSHomeDirectory())
            .appendingPathComponent("Library/Application Support")

        return applicationSupportDirectory
            .appendingPathComponent("Clicky", isDirectory: true)
            .appendingPathComponent("Secrets.json")
    }

    private static func resolveValue(fileKey: String, environmentKeys: [String]) -> String? {
        for environmentKey in environmentKeys {
            if let environmentValue = ProcessInfo.processInfo.environment[environmentKey] {
                let trimmedEnvironmentValue = environmentValue.trimmingCharacters(in: .whitespacesAndNewlines)
                if !trimmedEnvironmentValue.isEmpty {
                    return trimmedEnvironmentValue
                }
            }
        }

        if let fileValue = loadedSecretsFile[fileKey] {
            let trimmedFileValue = fileValue.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmedFileValue.isEmpty {
                return trimmedFileValue
            }
        }

        return nil
    }

    private static func readSecretsFile() -> [String: String] {
        guard let fileData = try? Data(contentsOf: secretsFileURL),
              let parsedJSON = try? JSONSerialization.jsonObject(with: fileData) as? [String: String] else {
            return [:]
        }
        return parsedJSON
    }
}
