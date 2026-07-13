// SPDX-License-Identifier: LGPL-3.0-or-later

import CryptoKit
import Darwin
import Foundation

package struct AssistantWebPageState: Codable, Equatable {
    package var url: String
    package var title: String
    package var mediaType: String
    package var text: String
    package var contentSha256: String
}

package enum AssistantWebResearchError: Error, Equatable {
    case unsafeURL
    case timeout
    case responseTooLarge
    case unsupportedResponse(String)
    case transport(String)
}

/// Host-mediated, credential-free public HTTPS retrieval for cited web evidence.
/// The provider sidecar receives only the bounded returned text, never a socket.
package struct AssistantWebResearchClient {
    private let maximumBytes = 1_048_576

    package init() {}

    package func fetch(_ url: URL) throws -> AssistantWebPageState {
        guard Self.isPublicHTTPS(url) else { throw AssistantWebResearchError.unsafeURL }
        let collector = AssistantWebCollector(maximumBytes: maximumBytes)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = 20
        configuration.timeoutIntervalForResource = 30
        configuration.httpCookieAcceptPolicy = .never
        configuration.httpShouldSetCookies = false
        configuration.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        let session = URLSession(configuration: configuration, delegate: collector, delegateQueue: nil)
        let task = session.dataTask(with: URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData))
        task.resume()
        guard collector.wait(timeout: 32) else {
            task.cancel()
            session.invalidateAndCancel()
            throw AssistantWebResearchError.timeout
        }
        session.finishTasksAndInvalidate()
        if let error = collector.error { throw error }
        guard let response = collector.response as? HTTPURLResponse,
              (200..<300).contains(response.statusCode),
              let finalURL = response.url,
              Self.isPublicHTTPS(finalURL)
        else { throw AssistantWebResearchError.unsupportedResponse("non-success response") }
        let mediaType = response.mimeType?.lowercased() ?? "application/octet-stream"
        guard mediaType.hasPrefix("text/")
                || ["application/json", "application/xml", "application/xhtml+xml"].contains(mediaType)
        else { throw AssistantWebResearchError.unsupportedResponse(mediaType) }
        let decoded = String(data: collector.data, encoding: .utf8)
            ?? String(data: collector.data, encoding: .isoLatin1)
            ?? ""
        let text = mediaType.contains("html") ? Self.plainText(decoded) : decoded
        let bounded = String(text.prefix(120_000))
        guard !bounded.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw AssistantWebResearchError.unsupportedResponse("empty text")
        }
        return AssistantWebPageState(
            url: finalURL.absoluteString,
            title: Self.title(decoded) ?? finalURL.host ?? finalURL.absoluteString,
            mediaType: mediaType,
            text: bounded,
            contentSha256: SHA256.hash(data: collector.data)
                .map { String(format: "%02x", $0) }.joined()
        )
    }

    package static func isPublicHTTPS(_ url: URL) -> Bool {
        guard url.scheme?.lowercased() == "https",
              url.user == nil,
              url.password == nil,
              let host = url.host?.lowercased(),
              !host.isEmpty,
              host != "localhost",
              !host.hasSuffix(".localhost"),
              !host.hasSuffix(".local")
        else { return false }
        var hints = addrinfo()
        hints.ai_family = AF_UNSPEC
        hints.ai_socktype = SOCK_STREAM
        var result: UnsafeMutablePointer<addrinfo>?
        guard getaddrinfo(host, "443", &hints, &result) == 0, let first = result else { return false }
        defer { freeaddrinfo(first) }
        var cursor: UnsafeMutablePointer<addrinfo>? = first
        var found = false
        while let current = cursor {
            guard let address = current.pointee.ai_addr else { return false }
            found = true
            if !isPublicAddress(address) { return false }
            cursor = current.pointee.ai_next
        }
        return found
    }

    private static func isPublicAddress(_ address: UnsafePointer<sockaddr>) -> Bool {
        switch Int32(address.pointee.sa_family) {
        case AF_INET:
            let ipv4 = address.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { pointer in
                UInt32(bigEndian: pointer.pointee.sin_addr.s_addr)
            }
            let first = ipv4 >> 24
            let second = (ipv4 >> 16) & 0xff
            return first != 0 && first != 10 && first != 127 && first < 224
                && !(first == 169 && second == 254)
                && !(first == 172 && (16...31).contains(second))
                && !(first == 192 && second == 168)
        case AF_INET6:
            let bytes = address.withMemoryRebound(to: sockaddr_in6.self, capacity: 1) { pointer in
                withUnsafeBytes(of: pointer.pointee.sin6_addr) { Array($0) }
            }
            let allZero = bytes.allSatisfy { $0 == 0 }
            let loopback = bytes.dropLast().allSatisfy { $0 == 0 } && bytes.last == 1
            let uniqueLocal = (bytes[0] & 0xfe) == 0xfc
            let linkLocal = bytes[0] == 0xfe && (bytes[1] & 0xc0) == 0x80
            let multicast = bytes[0] == 0xff
            return !allZero && !loopback && !uniqueLocal && !linkLocal && !multicast
        default:
            return false
        }
    }

    private static func title(_ html: String) -> String? {
        guard let range = html.range(of: "<title[^>]*>(.*?)</title>", options: [.regularExpression, .caseInsensitive])
        else { return nil }
        return plainText(String(html[range])).trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func plainText(_ html: String) -> String {
        html.replacingOccurrences(of: "(?is)<(script|style)[^>]*>.*?</\\1>", with: " ", options: .regularExpression)
            .replacingOccurrences(of: "(?s)<[^>]+>", with: " ", options: .regularExpression)
            .replacingOccurrences(of: "&nbsp;", with: " ")
            .replacingOccurrences(of: "&amp;", with: "&")
            .replacingOccurrences(of: "&lt;", with: "<")
            .replacingOccurrences(of: "&gt;", with: ">")
            .replacingOccurrences(of: "[ \\t\\r]+", with: " ", options: .regularExpression)
            .replacingOccurrences(of: "\\n{3,}", with: "\n\n", options: .regularExpression)
    }
}

private final class AssistantWebCollector: NSObject, URLSessionDataDelegate, URLSessionTaskDelegate,
    @unchecked Sendable
{
    private let maximumBytes: Int
    private let semaphore = DispatchSemaphore(value: 0)
    private let lock = NSLock()
    private var collected = Data()
    private var capturedResponse: URLResponse?
    private var capturedError: AssistantWebResearchError?

    init(maximumBytes: Int) { self.maximumBytes = maximumBytes }

    var data: Data { lock.withLock { collected } }
    var response: URLResponse? { lock.withLock { capturedResponse } }
    var error: AssistantWebResearchError? { lock.withLock { capturedError } }
    func wait(timeout: TimeInterval) -> Bool {
        semaphore.wait(timeout: .now() + timeout) == .success
    }

    func urlSession(
        _ session: URLSession,
        dataTask: URLSessionDataTask,
        didReceive response: URLResponse,
        completionHandler: @escaping (URLSession.ResponseDisposition) -> Void
    ) {
        if response.expectedContentLength > maximumBytes {
            lock.withLock { capturedError = .responseTooLarge }
            completionHandler(.cancel)
        } else {
            lock.withLock { capturedResponse = response }
            completionHandler(.allow)
        }
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        lock.withLock {
            guard capturedError == nil else { return }
            if collected.count + data.count > maximumBytes {
                capturedError = .responseTooLarge
                dataTask.cancel()
            } else {
                collected.append(data)
            }
        }
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        guard let url = request.url, AssistantWebResearchClient.isPublicHTTPS(url) else {
            lock.withLock { capturedError = .unsafeURL }
            completionHandler(nil)
            return
        }
        completionHandler(request)
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        lock.withLock {
            if capturedError == nil, let error {
                capturedError = .transport(error.localizedDescription)
            }
        }
        semaphore.signal()
    }
}
