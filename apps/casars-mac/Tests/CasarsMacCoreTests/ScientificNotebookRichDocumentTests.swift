import XCTest
@testable import CasarsMacCore

final class ScientificNotebookRichDocumentTests: XCTestCase {
    func testNoOpRoundTripPreservesUnsupportedMarkdownAndEOFNewlinesExactly() throws {
        let source = """
        # Styled heading
        
        <!-- an ordinary HTML comment
        whose bytes must stay untouched -->
        
        ```markdown
        # heading syntax inside a fence stays raw
        <!-- casa-rs-cell:v1 id=example kind=task -->
        ```toml
        [parameters]
        value = "example"
        ```
        <!-- /casa-rs-cell -->
        ```
        
            # indented Markdown stays raw
        
        Paragraph with  two  spaces.
        
        """

        let document = PrototypeNotebookRichDocument(markdown: source)

        XCTAssertEqual(document.markdown, source)
        XCTAssertEqual(document.elements.compactMap(\.taskID), [])
        XCTAssertEqual(document.elements.compactMap(\.headingLevel), [1])
        XCTAssertTrue(document.elements.contains { element in
            element.editableSource?.contains("ordinary HTML comment") == true
        })

        let reloaded = PrototypeNotebookRichDocument(markdown: document.markdown)
        XCTAssertEqual(reloaded.markdown, source)
        XCTAssertEqual(reloaded, document)
    }

    func testEditingBeforeBetweenAndAfterCellsChangesOnlySelectedProse() throws {
        let firstCell = taskCell(id: "first", trailingNewline: true)
        let secondCell = taskCell(id: "second", trailingNewline: true)
        let source = "Before one.\n\n"
            + firstCell
            + "\nBetween cells.\n\n"
            + secondCell
            + "\nAfter two.\n"
        var document = PrototypeNotebookRichDocument(markdown: source)
        let originalTaskSources = document.elements
            .filter { $0.taskID != nil }
            .map(\.source)

        let beforeID = try XCTUnwrap(document.elements.first {
            $0.editableSource?.contains("Before one.") == true
        }?.id)
        XCTAssertTrue(document.replaceEditableSource(
            elementID: beforeID,
            with: "Before revised.\n\n"
        ))
        let betweenID = try XCTUnwrap(document.elements.first {
            $0.editableSource?.contains("Between cells.") == true
        }?.id)
        XCTAssertTrue(document.replaceEditableSource(
            elementID: betweenID,
            with: "\nBetween revised.\n\n"
        ))
        let afterID = try XCTUnwrap(document.elements.first {
            $0.editableSource?.contains("After two.") == true
        }?.id)
        XCTAssertTrue(document.replaceEditableSource(
            elementID: afterID,
            with: "\nAfter revised.\n"
        ))

        let expected = "Before revised.\n\n"
            + firstCell
            + "\nBetween revised.\n\n"
            + secondCell
            + "\nAfter revised.\n"
        XCTAssertEqual(document.markdown, expected)
        XCTAssertEqual(
            document.elements.filter { $0.taskID != nil }.map(\.source),
            originalTaskSources
        )
        XCTAssertEqual(
            PrototypeNotebookRichDocument(markdown: document.markdown).markdown,
            expected
        )
    }

    func testStyledHeadingEditPreservesMarkerRawFallbackAndTaskCellBytes() throws {
        let cell = taskCell(id: "imager", trailingNewline: false)
        let untouchedRaw = """
        
        <!-- keep this -->
        ```text
        # not a styled heading
        ```

        """
        let source = "  ##\tOriginal heading  \r\n" + untouchedRaw + cell + "\n\nTail"
        var document = PrototypeNotebookRichDocument(markdown: source)
        let heading = try XCTUnwrap(document.elements.first { $0.headingLevel == 2 })
        let taskSource = try XCTUnwrap(document.elements.first { $0.taskID == "imager" }?.source)

        XCTAssertEqual(heading.editableSource, "Original heading  ")
        XCTAssertTrue(document.replaceEditableSource(
            elementID: heading.id,
            with: "Revised heading"
        ))

        XCTAssertEqual(
            document.markdown,
            "  ##\tRevised heading\r\n" + untouchedRaw + cell + "\n\nTail"
        )
        XCTAssertTrue(document.markdown.contains(untouchedRaw))
        XCTAssertEqual(document.elements.first { $0.taskID == "imager" }?.source, taskSource)
        XCTAssertFalse(document.replaceEditableSource(
            elementID: try XCTUnwrap(document.elements.first { $0.taskID == "imager" }?.id),
            with: "not allowed"
        ))
    }

    func testLeadingBetweenAndTrailingInsertionSurfacesAreInertUntilEdited() throws {
        let firstCell = taskCell(id: "first", trailingNewline: true)
        let secondCell = taskCell(id: "second", trailingNewline: false)
        let source = firstCell + secondCell
        var document = PrototypeNotebookRichDocument(markdown: source)
        let insertionIDs = document.elements
            .filter(\.isInsertionSurface)
            .map(\.id)

        XCTAssertEqual(insertionIDs.count, 3)
        XCTAssertEqual(document.markdown, source)
        XCTAssertEqual(
            document.elements.filter(\.isInsertionSurface).compactMap(\.editableSource),
            ["", "", ""]
        )

        XCTAssertTrue(document.replaceEditableSource(elementID: insertionIDs[0], with: "Lead"))
        XCTAssertTrue(document.replaceEditableSource(elementID: insertionIDs[1], with: "Middle"))
        XCTAssertTrue(document.replaceEditableSource(elementID: insertionIDs[2], with: "Tail"))

        XCTAssertEqual(
            document.markdown,
            "Lead\n\n" + firstCell + "\nMiddle\n\n" + secondCell + "\n\nTail"
        )
        XCTAssertEqual(
            document.elements.filter { $0.taskID != nil }.map(\.source),
            [firstCell, secondCell]
        )
        XCTAssertEqual(
            PrototypeNotebookRichDocument(markdown: document.markdown).markdown,
            document.markdown
        )
    }

    func testEmptyDocumentHasOneNonMutatingInsertionSurface() throws {
        var document = PrototypeNotebookRichDocument(markdown: "")
        let insertion = try XCTUnwrap(document.elements.only)

        XCTAssertTrue(insertion.isInsertionSurface)
        XCTAssertEqual(insertion.editableSource, "")
        XCTAssertEqual(document.markdown, "")

        XCTAssertTrue(document.replaceEditableSource(elementID: insertion.id, with: "First note"))
        XCTAssertEqual(document.markdown, "First note")
    }

    private func taskCell(id: String, trailingNewline: Bool) -> String {
        "<!-- casa-rs-cell:v1 id=\(id) kind=task -->\n"
            + "```toml\n"
            + "[parameters]\n"
            + "value = \"\(id)\"\n"
            + "```\n"
            + "<!-- /casa-rs-cell -->"
            + (trailingNewline ? "\n" : "")
    }
}

private extension Array {
    var only: Element? { count == 1 ? first : nil }
}
