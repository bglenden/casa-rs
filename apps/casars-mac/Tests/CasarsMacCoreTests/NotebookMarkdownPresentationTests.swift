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

    func testRichProjectionMaterializesParagraphsAndLists() throws {
        let source = """
        Open **Datasets** and select `twhya_calibrated.ms`.

        Check the following against the summary:

        - the observation has 68,335 records;
        - field 3 is the phase calibrator;
        - field 5 is TW Hya.

        My summary notes:
        """

        let attributed = try XCTUnwrap(NotebookMarkdownPresentation.attributedString(source))

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
}
