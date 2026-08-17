import Foundation
@testable import CasarsMacCore
import XCTest

final class MarkdownPresentationTests: XCTestCase {
    func testRichProjectionHidesControlCommentsAndParsesMarkdownDecoration() throws {
        let source = """
        <!-- casa-rs-notebook:v1 id=notebook-1 -->

        **Result:** use `simulate` with the selected array.

        <!-- casa-rs-ai-pin:v1 conversation=c1 message=m1 -->
        """

        let displayed = NotebookVisibleMarkdown.source(source)
        let attributed = try XCTUnwrap(NotebookVisibleMarkdown.attributedString(source))
        let renderedText = String(attributed.characters)

        XCTAssertFalse(displayed.contains("casa-rs-notebook"))
        XCTAssertFalse(displayed.contains("casa-rs-ai-pin"))
        XCTAssertFalse(renderedText.contains("**"))
        XCTAssertFalse(renderedText.contains("`"))
        XCTAssertEqual(renderedText, "Result: use simulate with the selected array.")
    }

    func testMetadataOnlyFragmentHasNoRichProjection() {
        let source = "<!-- casa-rs-notebook:v1 id=notebook-1 -->\n\n"

        XCTAssertEqual(NotebookVisibleMarkdown.source(source), "")
        XCTAssertNil(NotebookVisibleMarkdown.attributedString(source))
    }

    func testRichProjectionMaterializesParagraphsAndLists() throws {
        let source = """
        Open **Datasets** and select `twhya_calibrated.ms`.

        Check the following against the summary:

        - the observation has 68,335 records;
        - field 3 is the phase calibrator;
        - field 5 is TW Hya.

        My summary notes:
        """

        let attributed = try XCTUnwrap(NotebookVisibleMarkdown.attributedString(source))

        XCTAssertEqual(
            String(attributed.characters),
            """
            Open Datasets and select twhya_calibrated.ms.

            Check the following against the summary:

            • the observation has 68,335 records;
            • field 3 is the phase calibrator;
            • field 5 is TW Hya.

            My summary notes:
            """
        )
    }

    func testGenericMarkdownPreservesOrdinaryAndFencedComments() throws {
        let source = """
        <!-- ordinary HTML comment -->
        <!-- ordinary prose may mention casa-rs-cell without being control metadata -->

        ```markdown
        <!-- casa-rs-cell:v1 id=fenced kind=unknown -->
        ```
        """

        let generic = try XCTUnwrap(MarkdownPresentation.attributedString(source))
        XCTAssertTrue(String(generic.characters).contains("ordinary HTML comment"))
        XCTAssertTrue(String(generic.characters).contains("casa-rs-cell:v1 id=fenced kind=unknown"))
        let visible = NotebookVisibleMarkdown.source(source)
        XCTAssertTrue(visible.contains("ordinary HTML comment"))
        XCTAssertTrue(visible.contains("ordinary prose may mention casa-rs-cell"))
        XCTAssertTrue(visible.contains("casa-rs-cell:v1 id=fenced kind=unknown"))
    }

}
