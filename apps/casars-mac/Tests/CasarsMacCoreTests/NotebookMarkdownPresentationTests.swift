import Foundation
@testable import CasarsMacCore
import XCTest

final class NotebookMarkdownPresentationTests: XCTestCase {
    func testParagraphsAndTableRemainSeparateDisplayBlocks() throws {
        let source = """
        <!-- casa-rs-ai-pin:v1 conversation=c1 message=m1 -->
        First **paragraph**.

        Second paragraph.

        | Scan | RR peak | LL peak |
        | ---: | :--- | :---: |
        | 4 | ch. 126 | ch. 126 |
        | 26 | ch. 122 | ch. 120 |

        These are visibility amplitudes.
        """
        let blocks = NotebookMarkdownPresentation.blocks(source)
        XCTAssertEqual(blocks.count, 4)
        guard blocks.count == 4 else { return }
        XCTAssertEqual(String(blocks[0].text.characters), "First paragraph.")
        XCTAssertTrue(blocks[0].text.runs.contains {
            $0.inlinePresentationIntent?.contains(.stronglyEmphasized) == true
        })
        XCTAssertEqual(String(blocks[1].text.characters), "Second paragraph.")
        guard case .table(let columns) = blocks[2].kind else {
            return XCTFail("The table must have its own layout, not flattened text")
        }
        XCTAssertEqual(columns.map(\.alignment), [.right, .left, .center])
        XCTAssertEqual(blocks[2].children.map(\.kind), [
            .tableHeaderRow, .tableRow(rowIndex: 1), .tableRow(rowIndex: 2)
        ])
        XCTAssertEqual(blocks[2].children.map { row in
            row.children.map { String($0.text.characters) }
        }, [
            ["Scan", "RR peak", "LL peak"],
            ["4", "ch. 126", "ch. 126"],
            ["26", "ch. 122", "ch. 120"]
        ])
        XCTAssertEqual(String(blocks[3].text.characters), "These are visibility amplitudes.")
    }

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
        XCTAssertTrue(NotebookMarkdownPresentation.blocks(source).isEmpty)
    }

    func testNestedBlocksAndInlineStylesRetainTheirBoundaries() throws {
        let blocks = NotebookMarkdownPresentation.blocks("""
        > A [source](https://example.com).
        >
        > Another paragraph.

        3. First item
        4. Second item with `DATA`

        ```text
        first line
        second line
        ```
        """)
        XCTAssertEqual(blocks.count, 3)
        guard blocks.count == 3 else { return }
        XCTAssertEqual(blocks[0].kind, .blockQuote)
        XCTAssertEqual(blocks[0].children.count, 2)
        XCTAssertTrue(blocks[0].children[0].text.runs.contains {
            $0.link == URL(string: "https://example.com")
        })
        XCTAssertEqual(blocks[1].kind, .orderedList)
        XCTAssertEqual(blocks[1].children.map(\.kind), [.listItem(ordinal: 3), .listItem(ordinal: 4)])
        XCTAssertEqual(blocks[2].kind, .codeBlock(languageHint: "text"))
        XCTAssertEqual(String(blocks[2].text.characters), "first line\nsecond line\n")
    }
}
