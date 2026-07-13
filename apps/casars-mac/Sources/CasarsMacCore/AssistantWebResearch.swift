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
    package var untrustedEvidence: Bool
}

package enum AssistantWebResearchError: Error, Equatable {
    case unsafeURL
    case cancelled
    case timeout
    case responseTooLarge
    case unsupportedResponse(String)
    case transport(String)
}

/// Downloads one exactly approved public HTTPS resource without ambient
/// credentials, cookies, redirects, or an unbounded in-memory response.
package struct AssistantApprovedDownloadClient {
    private let maximumBytes = 134_217_728

    package init() {}

    package func download(
        _ url: URL,
        isCancelled: @escaping () -> Bool
    ) throws -> Data {
        guard AssistantWebResearchClient.isPublicHTTPS(url) else {
            throw AssistantWebResearchError.unsafeURL
        }
        let collector = AssistantWebCollector(maximumBytes: maximumBytes, allowsRedirects: false)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = 30
        configuration.timeoutIntervalForResource = 120
        configuration.httpCookieAcceptPolicy = .never
        configuration.httpShouldSetCookies = false
        configuration.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        configuration.urlCredentialStorage = nil
        let session = URLSession(configuration: configuration, delegate: collector, delegateQueue: nil)
        let task = session.dataTask(with: URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData))
        task.resume()
        let deadline = Date().addingTimeInterval(120)
        while !collector.wait(timeout: 0.1) {
            if isCancelled() {
                task.cancel()
                session.invalidateAndCancel()
                throw AssistantWebResearchError.cancelled
            }
            if Date() >= deadline {
                task.cancel()
                session.invalidateAndCancel()
                throw AssistantWebResearchError.timeout
            }
        }
        session.finishTasksAndInvalidate()
        if let error = collector.error { throw error }
        guard collector.connectionWasPublic else { throw AssistantWebResearchError.unsafeURL }
        guard let response = collector.response as? HTTPURLResponse,
              (200..<300).contains(response.statusCode),
              response.url == url
        else { throw AssistantWebResearchError.unsupportedResponse("non-success or redirected response") }
        return collector.data
    }
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
        guard collector.connectionWasPublic else { throw AssistantWebResearchError.unsafeURL }
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
                .map { String(format: "%02x", $0) }.joined(),
            untrustedEvidence: true
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

    package static func isPublicIPAddress(_ value: String) -> Bool {
        var ipv4 = in_addr()
        if value.withCString({ inet_pton(AF_INET, $0, &ipv4) }) == 1 {
            var address = sockaddr_in()
            address.sin_family = sa_family_t(AF_INET)
            address.sin_addr = ipv4
            return withUnsafePointer(to: &address) { pointer in
                pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    Self.isPublicAddress($0)
                }
            }
        }
        var ipv6 = in6_addr()
        if value.withCString({ inet_pton(AF_INET6, $0, &ipv6) }) == 1 {
            var address = sockaddr_in6()
            address.sin6_family = sa_family_t(AF_INET6)
            address.sin6_addr = ipv6
            return withUnsafePointer(to: &address) { pointer in
                pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    Self.isPublicAddress($0)
                }
            }
        }
        return false
    }

    private static func isPublicAddress(_ address: UnsafePointer<sockaddr>) -> Bool {
        switch Int32(address.pointee.sa_family) {
        case AF_INET:
            let ipv4 = address.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { pointer in
                UInt32(bigEndian: pointer.pointee.sin_addr.s_addr)
            }
            let first = ipv4 >> 24
            let second = (ipv4 >> 16) & 0xff
            let third = (ipv4 >> 8) & 0xff
            return first != 0 && first != 10 && first != 127 && first < 224
                && !(first == 169 && second == 254)
                && !(first == 172 && (16...31).contains(second))
                && !(first == 192 && second == 168)
                && !(first == 100 && (64...127).contains(second))
                && !(first == 192 && second == 0 && [0, 2].contains(third))
                && !(first == 192 && second == 88 && third == 99)
                && !(first == 198 && [18, 19, 51].contains(second))
                && !(first == 203 && second == 0 && third == 113)
        case AF_INET6:
            let bytes = address.withMemoryRebound(to: sockaddr_in6.self, capacity: 1) { pointer in
                withUnsafeBytes(of: pointer.pointee.sin6_addr) { Array($0) }
            }
            let globalUnicast = (bytes[0] & 0xe0) == 0x20
            let teredo = bytes[0...3] == [0x20, 0x01, 0x00, 0x00]
            let documentation = bytes[0...3] == [0x20, 0x01, 0x0d, 0xb8]
            let sixToFour = bytes[0] == 0x20 && bytes[1] == 0x02
            let orchid = bytes[0] == 0x20 && bytes[1] == 0x01
                && (bytes[2] & 0xf0) == 0x20
            return globalUnicast && !teredo && !documentation && !sixToFour && !orchid
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
    private let allowsRedirects: Bool
    private var observedRemoteAddresses: [String] = []
    private var observedProxy = false
    private var observedMetrics = false

    init(maximumBytes: Int, allowsRedirects: Bool = true) {
        self.maximumBytes = maximumBytes
        self.allowsRedirects = allowsRedirects
    }

    var data: Data { lock.withLock { collected } }
    var response: URLResponse? { lock.withLock { capturedResponse } }
    var error: AssistantWebResearchError? { lock.withLock { capturedError } }
    var connectionWasPublic: Bool {
        lock.withLock {
            observedMetrics && !observedProxy && !observedRemoteAddresses.isEmpty
                && observedRemoteAddresses.allSatisfy(AssistantWebResearchClient.isPublicIPAddress)
        }
    }
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
        guard allowsRedirects,
              let url = request.url,
              AssistantWebResearchClient.isPublicHTTPS(url)
        else {
            lock.withLock { capturedError = .unsafeURL }
            completionHandler(nil)
            return
        }
        completionHandler(request)
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didFinishCollecting metrics: URLSessionTaskMetrics
    ) {
        lock.withLock {
            observedMetrics = true
            observedProxy = metrics.transactionMetrics.contains(\.isProxyConnection)
            observedRemoteAddresses = metrics.transactionMetrics.compactMap(\.remoteAddress)
        }
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
