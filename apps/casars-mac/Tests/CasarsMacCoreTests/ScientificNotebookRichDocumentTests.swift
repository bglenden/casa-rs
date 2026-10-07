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

        let document = try PrototypeNotebookRichProjectionAdapter.document(markdown: source)

        XCTAssertEqual(document.markdown, source)
        XCTAssertEqual(document.blocks.compactMap(\.managedCellID), [])
        XCTAssertEqual(document.blocks.compactMap(\.headingLevel), [1])
        XCTAssertTrue(document.blocks.contains { element in
            element.editableSource?.contains("ordinary HTML comment") == true
        })

        let reloaded = try PrototypeNotebookRichProjectionAdapter.document(markdown: document.markdown)
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
        var document = try PrototypeNotebookRichProjectionAdapter.document(markdown: source)
        let originalTaskSources = document.blocks
            .filter { $0.managedCellID != nil }
            .map(\.source)

        let beforeID = try XCTUnwrap(document.blocks.first {
            $0.editableSource?.contains("Before one.") == true
        }?.id)
        XCTAssertTrue(document.replaceEditableSource(
            blockID: beforeID,
            with: "Before revised.\n\n"
        ))
        let betweenID = try XCTUnwrap(document.blocks.first {
            $0.editableSource?.contains("Between cells.") == true
        }?.id)
        XCTAssertTrue(document.replaceEditableSource(
            blockID: betweenID,
            with: "\nBetween revised.\n\n"
        ))
        let afterID = try XCTUnwrap(document.blocks.first {
            $0.editableSource?.contains("After two.") == true
        }?.id)
        XCTAssertTrue(document.replaceEditableSource(
            blockID: afterID,
            with: "\nAfter revised.\n"
        ))

        let expected = "Before revised.\n\n"
            + firstCell
            + "\nBetween revised.\n\n"
            + secondCell
            + "\nAfter revised.\n"
        XCTAssertEqual(document.markdown, expected)
        XCTAssertEqual(
            document.blocks.filter { $0.managedCellID != nil }.map(\.source),
            originalTaskSources
        )
        XCTAssertEqual(
            try PrototypeNotebookRichProjectionAdapter.document(markdown: document.markdown).markdown,
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
        var document = try PrototypeNotebookRichProjectionAdapter.document(markdown: source)
        let heading = try XCTUnwrap(document.blocks.first { $0.headingLevel == 2 })
        let taskSource = try XCTUnwrap(document.blocks.first { $0.managedCellID == "imager" }?.source)

        XCTAssertEqual(heading.editableSource, "Original heading  ")
        XCTAssertTrue(document.replaceEditableSource(
            blockID: heading.id,
            with: "Revised heading"
        ))

        XCTAssertEqual(
            document.markdown,
            "  ##\tRevised heading\r\n" + untouchedRaw + cell + "\n\nTail"
        )
        XCTAssertTrue(document.markdown.contains(untouchedRaw))
        XCTAssertEqual(document.blocks.first { $0.managedCellID == "imager" }?.source, taskSource)
        XCTAssertFalse(document.replaceEditableSource(
            blockID: try XCTUnwrap(document.blocks.first { $0.managedCellID == "imager" }?.id),
            with: "not allowed"
        ))
    }

    func testLeadingBetweenAndTrailingInsertionSurfacesAreInertUntilEdited() throws {
        let firstCell = taskCell(id: "first", trailingNewline: true)
        let secondCell = taskCell(id: "second", trailingNewline: false)
        let source = firstCell + secondCell
        var document = try PrototypeNotebookRichProjectionAdapter.document(markdown: source)
        let insertionIDs = document.blocks
            .filter(\.isInsertionSurface)
            .map(\.id)

        XCTAssertEqual(insertionIDs.count, 3)
        XCTAssertEqual(document.markdown, source)
        XCTAssertEqual(
            document.blocks.filter(\.isInsertionSurface).compactMap(\.editableSource),
            ["", "", ""]
        )

        XCTAssertTrue(document.replaceEditableSource(blockID: insertionIDs[0], with: "Lead"))
        XCTAssertTrue(document.replaceEditableSource(blockID: insertionIDs[1], with: "Middle"))
        XCTAssertTrue(document.replaceEditableSource(blockID: insertionIDs[2], with: "Tail"))

        XCTAssertEqual(
            document.markdown,
            "Lead\n\n" + firstCell + "\nMiddle\n\n" + secondCell + "\n\nTail"
        )
        XCTAssertEqual(
            document.blocks.filter { $0.managedCellID != nil }.map(\.source),
            [firstCell, secondCell]
        )
        XCTAssertEqual(
            try PrototypeNotebookRichProjectionAdapter.document(markdown: document.markdown).markdown,
            document.markdown
        )
    }

    func testEmptyDocumentHasOneNonMutatingInsertionSurface() throws {
        var document = try PrototypeNotebookRichProjectionAdapter.document(markdown: "")
        let insertion = try XCTUnwrap(document.blocks.only)

        XCTAssertTrue(insertion.isInsertionSurface)
        XCTAssertEqual(insertion.editableSource, "")
        XCTAssertEqual(document.markdown, "")

        XCTAssertTrue(document.replaceEditableSource(blockID: insertion.id, with: "First note"))
        XCTAssertEqual(document.markdown, "First note")
    }

    func testPlainRichEditsCannotConsumeAdjacentTaskCellBoundaries() throws {
        let firstCell = taskCell(id: "first", trailingNewline: true)
        let secondCell = taskCell(id: "second", trailingNewline: true)
        let source = "Before.\n\n" + firstCell + "\nBetween.\n\n" + secondCell + "\nAfter."
        var document = try PrototypeNotebookRichProjectionAdapter.document(markdown: source)

        for (needle, replacement) in [
            ("Before.", "Before revised."),
            ("Between.", "Between revised."),
            ("After.", "After revised."),
        ] {
            let elementID = try XCTUnwrap(document.blocks.first {
                $0.editableSource?.contains(needle) == true
            }?.id)
            XCTAssertTrue(document.replaceEditableSource(blockID: elementID, with: replacement))
        }

        let reloaded = try PrototypeNotebookRichProjectionAdapter.document(markdown: document.markdown)
        XCTAssertEqual(reloaded.blocks.compactMap(\.managedCellID), ["first", "second"])
        XCTAssertTrue(document.markdown.contains("Before revised.\n\n<!-- casa-rs-cell:v1 id=first"))
        XCTAssertTrue(document.markdown.contains("<!-- /casa-rs-cell -->\n\nBetween revised."))
        XCTAssertTrue(document.markdown.hasSuffix("<!-- /casa-rs-cell -->\n\nAfter revised."))
    }

    func testVisualizationOutputCellRemainsManagedAcrossRichModeEdits() throws {
        let outputCell = """
        <!-- casa-rs-cell:v1 id=plot-output kind=output -->
        <!-- casa-rs-visualization:v1 id=saved-plot -->
        Latest revision is shown by default. Expand Previous revisions for history.
        <!-- /casa-rs-cell -->
        """ + "\n"
        let source = "Before.\n\n" + outputCell + "\nAfter."
        var document = try PrototypeNotebookRichProjectionAdapter.document(markdown: source)

        XCTAssertEqual(document.blocks.compactMap(\.managedCellID), ["plot-output"])
        XCTAssertEqual(
            document.blocks.first { $0.managedCellID == "plot-output" }?.source,
            outputCell
        )

        let afterID = try XCTUnwrap(document.blocks.first {
            $0.editableSource?.contains("After.") == true
        }?.id)
        XCTAssertTrue(document.replaceEditableSource(blockID: afterID, with: "After revised."))

        XCTAssertTrue(document.markdown.contains(outputCell))
        XCTAssertEqual(
            try PrototypeNotebookRichProjectionAdapter.document(markdown: document.markdown)
                .blocks.compactMap(\.managedCellID),
            ["plot-output"]
        )
    }

    func testUnresolvedManagedOutputFallbackHidesControlMarkers() throws {
        let source = """
        <!-- casa-rs-cell:v1 id=acquisition kind=output -->
        Recorded operation `tutorial.acquire.twhya-calibrated`. Managed execution details are stored separately.
        <!-- /casa-rs-cell -->
        """
        let document = try PrototypeNotebookRichProjectionAdapter.document(markdown: source)
        let managedElement = try XCTUnwrap(document.blocks.first { $0.managedCellID == "acquisition" })

        XCTAssertEqual(
            NotebookVisibleMarkdown.source(managedElement.source).trimmingCharacters(in: .whitespacesAndNewlines),
            "Recorded operation `tutorial.acquire.twhya-calibrated`. Managed execution details are stored separately."
        )
    }

    func testPrototypeAdapterProjectsUnknownKindsAndExactUTF8BodyRanges() throws {
        let source = "πreamble\n"
            + "<!-- casa-rs-cell:v1 id=unknown kind=future-kind -->\n"
            + "café body\n"
            + "<!-- /casa-rs-cell -->\n"
        let projection = try PrototypeNotebookRichProjectionAdapter.projection(markdown: source)
        let cell = try XCTUnwrap(projection.cells.only)

        XCTAssertEqual(cell.id, "unknown")
        XCTAssertEqual(cell.kind, "future-kind")
        XCTAssertEqual(
            utf8Slice(source, start: cell.bodyStart, end: cell.bodyEnd),
            "café body\n"
        )
        XCTAssertEqual(
            utf8Slice(source, start: cell.fullStart, end: cell.fullEnd),
            "<!-- casa-rs-cell:v1 id=unknown kind=future-kind -->\n"
                + "café body\n"
                + "<!-- /casa-rs-cell -->\n"
        )
        XCTAssertEqual(NotebookRichDocument(projection: projection).markdown, source)
    }

    func testValidatedProductionProjectionRejectsUnsafeRanges() {
        let source = "é\nbody\n"
        let valid = NotebookManagedCellSpan(
            id: "cell",
            kind: "unknown",
            fullStart: 0,
            fullEnd: UInt64(source.utf8.count),
            bodyStart: 3,
            bodyEnd: UInt64(source.utf8.count)
        )

        XCTAssertThrowsError(
            try NotebookRichSourceProjection(
                source: source,
                cells: [NotebookManagedCellSpan(
                    id: "cell",
                    kind: "unknown",
                    fullStart: 1,
                    fullEnd: valid.fullEnd,
                    bodyStart: valid.bodyStart,
                    bodyEnd: valid.bodyEnd
                )]
            )
        ) { error in
            XCTAssertEqual(
                error as? NotebookRichProjectionError,
                .nonUTF8Boundary(cellID: "cell", offset: 1)
            )
        }

        XCTAssertThrowsError(
            try NotebookRichSourceProjection(
                source: source,
                cells: [valid, NotebookManagedCellSpan(
                    id: "cell",
                    kind: "unknown",
                    fullStart: valid.fullStart,
                    fullEnd: valid.fullEnd,
                    bodyStart: valid.bodyStart,
                    bodyEnd: valid.bodyEnd
                )]
            )
        ) { error in
            XCTAssertEqual(error as? NotebookRichProjectionError, .duplicateCellID("cell"))
        }

        XCTAssertThrowsError(
            try NotebookRichSourceProjection(
                source: source,
                cells: [NotebookManagedCellSpan(
                    id: "out-of-bounds",
                    kind: "unknown",
                    fullStart: 0,
                    fullEnd: UInt64(source.utf8.count + 1),
                    bodyStart: 3,
                    bodyEnd: UInt64(source.utf8.count)
                )]
            )
        ) { error in
            XCTAssertEqual(
                error as? NotebookRichProjectionError,
                .outOfBounds(
                    cellID: "out-of-bounds",
                    range: "full",
                    sourceByteCount: source.utf8.count
                )
            )
        }

        let overlapSource = "abcdefghij"
        XCTAssertThrowsError(
            try NotebookRichSourceProjection(
                source: overlapSource,
                cells: [
                    NotebookManagedCellSpan(
                        id: "first",
                        kind: "unknown",
                        fullStart: 0,
                        fullEnd: 5,
                        bodyStart: 1,
                        bodyEnd: 4
                    ),
                    NotebookManagedCellSpan(
                        id: "second",
                        kind: "unknown",
                        fullStart: 4,
                        fullEnd: 9,
                        bodyStart: 5,
                        bodyEnd: 8
                    ),
                ]
            )
        ) { error in
            XCTAssertEqual(
                error as? NotebookRichProjectionError,
                .overlappingFullRanges(cellID: "second")
            )
        }
    }

    func testDuplicateManagedBodiesRemainDistinctSourceExactBlocks() throws {
        let first = duplicateBodyCell(id: "first")
        let second = duplicateBodyCell(id: "second")
        let source = first + second
        let projection = try PrototypeNotebookRichProjectionAdapter.projection(markdown: source)
        let bodySlices = projection.cells.map {
            utf8Slice(source, start: $0.bodyStart, end: $0.bodyEnd)
        }
        XCTAssertEqual(bodySlices, ["same body\n", "same body\n"])

        let document = NotebookRichDocument(projection: projection)
        XCTAssertEqual(document.blocks.compactMap(\.managedCellID), ["first", "second"])
        XCTAssertEqual(document.markdown, source)
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

    private func duplicateBodyCell(id: String) -> String {
        "<!-- casa-rs-cell:v1 id=\(id) kind=task -->\n"
            + "same body\n"
            + "<!-- /casa-rs-cell -->\n"
    }

    private func utf8Slice(_ source: String, start: UInt64, end: UInt64) -> String {
        let lower = source.utf8.index(source.utf8.startIndex, offsetBy: Int(start))
        let upper = source.utf8.index(source.utf8.startIndex, offsetBy: Int(end))
        return String(decoding: source.utf8[lower..<upper], as: UTF8.self)
    }
}

private extension Array {
    var only: Element? { count == 1 ? first : nil }
}
