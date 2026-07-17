import Foundation

/// Display-only projection used by rich notebook and assistant surfaces.
///
/// The persisted source remains ordinary Markdown. CASA-RS control comments are
/// deliberately removed only from this projection so Rich mode never exposes
/// implementation metadata while Raw mode remains a lossless editor.
package enum NotebookMarkdownPresentation {
    package static func displaySource(_ source: String) -> String {
        let range = NSRange(source.startIndex..., in: source)
        let withoutControlComments = controlCommentPattern.stringByReplacingMatches(
            in: source,
            range: range,
            withTemplate: ""
        )
        return withoutControlComments.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    package static func attributedString(_ source: String) -> AttributedString? {
        let displayed = displaySource(source)
        guard !displayed.isEmpty else { return nil }
        return (try? AttributedString(
            markdown: displayed,
            options: AttributedString.MarkdownParsingOptions(interpretedSyntax: .full)
        )) ?? AttributedString(displayed)
    }

    private static let controlCommentPattern = try! NSRegularExpression(
        pattern: #"<!--[\s\S]*?-->"#
    )
}
