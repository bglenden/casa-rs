import AppKit
import XCTest

final class CasarsMacUITests: XCTestCase {
    private var app: XCUIApplication!

    override func setUpWithError() throws {
        continueAfterFailure = false
    }

    override func tearDownWithError() throws {
        if let testRun, testRun.failureCount > 0, app != nil {
            let screenshot = XCTAttachment(screenshot: app.screenshot())
            screenshot.name = "Failure screenshot"
            screenshot.lifetime = .keepAlways
            add(screenshot)

            let hierarchy = XCTAttachment(string: app.debugDescription)
            hierarchy.name = "Failure accessibility hierarchy"
            hierarchy.lifetime = .keepAlways
            add(hierarchy)
        }
        app?.terminate()
        app = nil
    }

    func testCompleteDocumentEditingAndTaskProjection() throws {
        launchPrototype()

        selectViewMode("Raw")
        let rawEditor = try require("notebook.editor.raw")
        let originalMarkdown = try textValue(rawEditor)
        let originalTaskCells = taskCells(in: originalMarkdown)
        XCTAssertEqual(originalTaskCells.count, 3)

        selectViewMode("Rich")
        let documentScroll = app.scrollViews["notebook.document.scroll"]
        XCTAssertTrue(documentScroll.exists)
        documentScroll.scroll(byDeltaX: 0, deltaY: -1_500)
        replaceText("notebook.richElement.rich-element-9", with: "After the final task cell — edited by XCUITest.")
        documentScroll.scroll(byDeltaX: 0, deltaY: 600)
        replaceText("notebook.richElement.rich-element-5", with: "Between task cells — this is deliberately not the first note.")
        documentScroll.scroll(byDeltaX: 0, deltaY: 900)
        replaceText("notebook.richElement.rich-element-3", with: "Before the first task cell — edited by XCUITest.")

        XCTAssertEqual(try accessibilityValue("notebook.dirtyState"), "dirty")
        selectViewMode("Raw")
        let richEditsMarkdown = try textValue(try require("notebook.editor.raw"))
        XCTAssertTrue(richEditsMarkdown.contains("Before the first task cell — edited by XCUITest."))
        XCTAssertTrue(richEditsMarkdown.contains("this is deliberately not the first note"))
        XCTAssertTrue(richEditsMarkdown.contains("After the final task cell — edited by XCUITest."))
        XCTAssertEqual(taskCells(in: richEditsMarkdown), originalTaskCells, "Rich prose edits must not rewrite typed task cells.")

        let taskEditedMarkdown = richEditsMarkdown.replacingOccurrences(
            of: "niter = 1000",
            with: "niter = 250"
        )
        XCTAssertNotEqual(taskEditedMarkdown, richEditsMarkdown)
        replaceText("notebook.editor.raw", with: taskEditedMarkdown)
        selectViewMode("Rich")

        let parameterBlock = try require("notebook.parameters.open.receipt-imager-mfs")
        XCTAssertTrue(parameterBlock.debugDescription.contains("250"), parameterBlock.debugDescription)
        parameterBlock.click()
        XCTAssertTrue(try require("central.tab.tab-prototype-task-receipt-imager-mfs").waitForExistence(timeout: 5))
        XCTAssertTrue(try require("prototypeTask.identity.receipt-imager-mfs").exists)
        XCTAssertEqual(try textValue(try require("prototypeTask.parameter.niter")), "250")

        try require("central.tab.tab-scientific-notebook").click()
        try require("notebook.save").click()
        XCTAssertEqual(try accessibilityValue("notebook.dirtyState"), "saved")
        assertZeroProductionBoundaryCalls()
    }

    func testNamedNotebookDraftsRemainIndependent() throws {
        launchPrototype()

        replaceText("notebook.richElement.rich-element-3", with: "Analysis-only draft note.")
        notebookSelector("notebook-twhya-observation-log").click()
        XCTAssertTrue(try require("notebook.parameters.open.receipt-listobs-summary").waitForExistence(timeout: 5))
        replaceText("notebook.richElement.rich-element-3", with: "Observation-log-only draft note.")

        notebookSelector("notebook-twhya-analysis").click()
        XCTAssertTrue(waitForValue("notebook.richElement.rich-element-3", containing: "Analysis-only draft note"))
        XCTAssertFalse(try textValue(try require("notebook.richElement.rich-element-3")).contains("Observation-log-only"))

        notebookSelector("notebook-twhya-observation-log").click()
        XCTAssertTrue(waitForValue("notebook.richElement.rich-element-3", containing: "Observation-log-only draft note"))
        assertZeroProductionBoundaryCalls()
    }

    func testTaskNavigationExecutionGesturesAndNeutralTaskCell() throws {
        launchPrototype()

        let parameterBlock = try require("notebook.parameters.open.receipt-imager-mfs")
        let status = try require("notebook.executionStatus.receipt-imager-mfs")
        XCTAssertGreaterThan(parameterBlock.frame.width, status.frame.width * 2, "Status color/label must remain a compact affordance, not task-cell decoration.")
        XCTAssertNotEqual(parameterBlock.identifier, status.identifier)

        parameterBlock.click()
        XCTAssertTrue(try require("prototypeTask.identity.receipt-imager-mfs").waitForExistence(timeout: 5))
        XCTAssertEqual(try textValue(try require("prototypeTask.parameter.vis")), "data/twhya_calibrated.ms")

        try require("central.tab.tab-scientific-notebook").click()
        status.click()
        XCTAssertTrue(try require("notebook.execution.restart.receipt-imager-mfs").waitForExistence(timeout: 3))
        XCTAssertTrue(try accessibilityValue("notebook.executionStatus.receipt-imager-mfs").contains("expanded"))

        status.doubleClick()
        XCTAssertTrue(try require("central.tab.tab-prototype-task-receipt-imager-mfs").waitForExistence(timeout: 5))
        assertZeroProductionBoundaryCalls()
    }

    func testFixtureRestartCompletionCancellationAndIsolation() throws {
        launchPrototype()

        let status = try require("notebook.executionStatus.receipt-imager-mfs")
        status.click()
        try require("notebook.execution.restart.receipt-imager-mfs").click()
        XCTAssertTrue(waitForAccessibilityValue("notebook.executionStatus.receipt-imager-mfs", containing: "Running"))
        try require("notebook.execution.complete.receipt-imager-mfs").click()
        XCTAssertTrue(waitForAccessibilityValue("notebook.executionStatus.receipt-imager-mfs", containing: "Succeeded"))

        try require("notebook.execution.restart.receipt-imager-mfs").click()
        XCTAssertTrue(waitForAccessibilityValue("notebook.executionStatus.receipt-imager-mfs", containing: "Running"))
        try require("notebook.execution.cancel.receipt-imager-mfs").click()
        XCTAssertTrue(waitForAccessibilityValue("notebook.executionStatus.receipt-imager-mfs", containing: "Cancelled"))
        XCTAssertTrue(try textValue(try require("notebook.execution.revisionCount.receipt-imager-mfs")).contains("3 revisions"))

        let workbenchMenu = app.menuBars.menuBarItems["Workbench"]
        XCTAssertTrue(workbenchMenu.exists)
        workbenchMenu.click()
        for prefix in ["Open Project Directory", "Open Tutorial Pack", "Open Demo Project"] {
            let item = app.menuItems.matching(NSPredicate(format: "title BEGINSWITH %@", prefix)).firstMatch
            XCTAssertTrue(item.exists, "Missing menu item beginning with \(prefix)")
            XCTAssertFalse(item.isEnabled, "\(prefix) must be disabled in the isolated prototype runtime")
        }
        app.typeKey(.escape, modifierFlags: [])

        try require("central.tab.plus").click()
        let datasetExplorer = app.menuItems["Dataset Explorer"]
        XCTAssertTrue(datasetExplorer.waitForExistence(timeout: 2))
        datasetExplorer.click()
        XCTAssertFalse(app.descendants(matching: .any).matching(identifier: "central.tab.dataset-prototype-twhya-ms").firstMatch.exists)
        assertZeroProductionBoundaryCalls()
    }

    func testExternalConflictKeepLocalDraft() throws {
        launchPrototype(scenario: "external-conflict")

        XCTAssertTrue(try require("notebook.conflict.keepDraft").exists)
        try require("notebook.conflict.keepDraft").click()
        XCTAssertFalse(element("notebook.conflict.reloadExternal").exists)
        selectViewMode("Raw")
        XCTAssertTrue(try textValue(try require("notebook.editor.raw")).contains("Local unsaved note: compare the robust-weighting runs."))
        XCTAssertEqual(try accessibilityValue("notebook.dirtyState"), "dirty")
        assertZeroProductionBoundaryCalls()
    }

    func testExternalConflictReloadExternalVersion() throws {
        launchPrototype(scenario: "external-conflict")

        try require("notebook.conflict.reloadExternal").click()
        XCTAssertFalse(element("notebook.conflict.keepDraft").exists)
        selectViewMode("Raw")
        XCTAssertFalse(try textValue(try require("notebook.editor.raw")).contains("Local unsaved note: compare the robust-weighting runs."))
        XCTAssertEqual(try accessibilityValue("notebook.dirtyState"), "saved")
        assertZeroProductionBoundaryCalls()
    }

    func testWaveOneAccessibilityAudit() throws {
        launchPrototype()
        try app.performAccessibilityAudit { issue in
            if issue.auditType.contains(.contrast),
               (issue.element?.label == "casa-rs Workbench"
                   || issue.element?.value as? String == "casa-rs Workbench")
            {
                return true
            }
            if ["split.resizeHandle", "central.tab.plus"].contains(issue.element?.identifier) {
                return true
            }
            if issue.element?.elementType == .group
                || issue.element?.elementType == .touchBar
            {
                return true
            }
            guard issue.auditType.contains(.contrast),
                  let identifier = issue.element?.identifier
            else { return false }
            return identifier == "notebook.boundaryAudit"
                || identifier.hasPrefix("notebook.selector.")
        }
        assertZeroProductionBoundaryCalls()
    }

    private func launchPrototype(scenario: String = "happy-path") {
        app = XCUIApplication()
        app.launchArguments = [
            "-ApplePersistenceIgnoreState", "YES",
            "--show-prototype", "notebook",
            "--prototype-state", scenario,
        ]
        app.launch()
        XCTAssertTrue(
            app.windows["casa-rs Workbench"].waitForExistence(timeout: 10),
            app.debugDescription
        )
        XCTAssertTrue(app.buttons["dock.mode.notebooks"].exists)
    }

    private func require(_ identifier: String, timeout: TimeInterval = 5) throws -> XCUIElement {
        let result = element(identifier)
        XCTAssertTrue(result.waitForExistence(timeout: timeout), "Missing accessibility identifier \(identifier)\n\(app.debugDescription)")
        return result
    }

    private func element(_ identifier: String) -> XCUIElement {
        app.descendants(matching: .any).matching(identifier: identifier).firstMatch
    }

    private func notebookSelector(_ notebookID: String) -> XCUIElement {
        app.staticTexts.matching(identifier: "notebook.selector.\(notebookID)").firstMatch
    }

    private func selectViewMode(_ label: String) {
        let identifier = "notebook.viewMode.\(label.lowercased())"
        let segment = app.radioButtons[identifier]
        XCTAssertTrue(segment.waitForExistence(timeout: 5), app.debugDescription)
        segment.click()
    }

    private func replaceText(_ identifier: String, with value: String) {
        let result = element(identifier)
        XCTAssertTrue(result.waitForExistence(timeout: 5), "Missing editable element \(identifier)")
        result.click()
        result.typeKey("a", modifierFlags: .command)
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        XCTAssertTrue(pasteboard.setString(value, forType: .string))
        result.typeKey("v", modifierFlags: .command)
        if identifier == "notebook.editor.raw" {
            XCTAssertTrue(
                waitForTextValue(result, equalTo: value),
                "Raw editor did not commit the complete pasted document"
            )
            result.typeKey(.tab, modifierFlags: [])
            Thread.sleep(forTimeInterval: 0.2)
        } else {
            XCTAssertTrue(
                waitForTextValue(result, containing: value),
                "Editable element \(identifier) did not commit the pasted value"
            )
        }
    }

    private func textValue(_ element: XCUIElement) throws -> String {
        try XCTUnwrap(element.value as? String, "Expected text value for \(element.identifier): \(element.debugDescription)")
    }

    private func accessibilityValue(_ identifier: String) throws -> String {
        try textValue(try require(identifier))
    }

    private func waitForValue(_ identifier: String, containing substring: String) -> Bool {
        let element = app.descendants(matching: .any).matching(identifier: identifier).firstMatch
        let predicate = NSPredicate(format: "value CONTAINS %@", substring)
        return XCTWaiter.wait(for: [XCTNSPredicateExpectation(predicate: predicate, object: element)], timeout: 5) == .completed
    }

    private func waitForTextValue(_ element: XCUIElement, equalTo expected: String) -> Bool {
        let deadline = Date().addingTimeInterval(5)
        repeat {
            if element.value as? String == expected {
                return true
            }
            Thread.sleep(forTimeInterval: 0.05)
        } while Date() < deadline
        return false
    }

    private func waitForTextValue(_ element: XCUIElement, containing expected: String) -> Bool {
        let deadline = Date().addingTimeInterval(5)
        repeat {
            if (element.value as? String)?.contains(expected) == true {
                return true
            }
            Thread.sleep(forTimeInterval: 0.05)
        } while Date() < deadline
        return false
    }

    private func waitForAccessibilityValue(_ identifier: String, containing substring: String) -> Bool {
        waitForValue(identifier, containing: substring)
    }

    private func assertZeroProductionBoundaryCalls() {
        let audit = app.descendants(matching: .any).matching(identifier: "notebook.boundaryAudit").firstMatch
        XCTAssertTrue(audit.waitForExistence(timeout: 3), app.debugDescription)
        XCTAssertEqual(audit.value as? String ?? audit.label, "0")
    }

    private func taskCells(in markdown: String) -> [String] {
        let opening = "<!-- casa-rs-cell:v1 "
        let closing = "<!-- /casa-rs-cell -->"
        var cells: [String] = []
        var remainder = markdown[...]
        while let start = remainder.range(of: opening)?.lowerBound,
              let endMarker = remainder[start...].range(of: closing)
        {
            let end = remainder.index(endMarker.upperBound, offsetBy: 0)
            cells.append(String(remainder[start..<end]))
            remainder = remainder[end...]
        }
        return cells
    }
}
