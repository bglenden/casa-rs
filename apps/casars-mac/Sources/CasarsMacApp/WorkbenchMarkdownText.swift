import CasarsMacCore
import SwiftUI

/// The one generic SwiftUI Markdown renderer used by notebook prose,
/// unresolved managed bodies, and assistant discussion messages.
struct WorkbenchMarkdownText: View {
    let source: String

    var body: some View {
        if let rendered = MarkdownPresentation.attributedString(source) {
            Text(rendered)
        } else {
            EmptyView()
        }
    }
}
