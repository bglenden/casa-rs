import Foundation
@testable import CasarsMacCore
import XCTest

final class NotebookMarkdownPresentationTests: XCTestCase {
    func testRichProjectionHidesControlCommentsAndParsesMarkdownDecoration() throws {
        let source = """
        <!-- casa-rs-notebook:v1 id=notebook-1 -->

        **Result:** use `simulate` with the selected array.

        <!-- casa-rs-ai-pin:v1 conversation=c1 message=m1 -->
        """

        let displayed = NotebookMarkdownPresentation.displaySource(source)
        let attributed = try XCTUnwrap(NotebookMarkdownPresentation.attributedString(source))
        let renderedText = String(attributed.characters)

        XCTAssertFalse(displayed.contains("casa-rs-notebook"))
        XCTAssertFalse(displayed.contains("casa-rs-ai-pin"))
        XCTAssertFalse(renderedText.contains("**"))
        XCTAssertFalse(renderedText.contains("`"))
        XCTAssertEqual(renderedText, "Result: use simulate with the selected array.")
    }

    func testMetadataOnlyFragmentHasNoRichProjection() {
        let source = "<!-- casa-rs-notebook:v1 id=notebook-1 -->\n\n"

        XCTAssertEqual(NotebookMarkdownPresentation.displaySource(source), "")
        XCTAssertNil(NotebookMarkdownPresentation.attributedString(source))
    }
}
