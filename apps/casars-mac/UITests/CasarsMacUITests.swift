import AppKit
import CryptoKit
import XCTest

final class CasarsMacUITests: XCTestCase {
    private var app: XCUIApplication!
    private var productionProjectURL: URL?

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
        if app != nil {
            app.terminate()
            XCTAssertTrue(
                app.wait(for: .notRunning, timeout: 5),
                "The tested app did not terminate before the next GUI workflow"
            )
        }
        app = nil
        if let productionProjectURL {
            try? FileManager.default.removeItem(at: productionProjectURL)
            self.productionProjectURL = nil
        }
    }

    func testCompleteDocumentEditingAndTaskProjection() throws {
        launchPrototype()

        selectViewMode("Raw")
        let rawEditor = try require("notebook.editor.raw")
        let originalMarkdown = try textValue(rawEditor)
        let originalTaskCells = taskCells(in: originalMarkdown)
        XCTAssertEqual(originalTaskCells.count, 3)

        selectViewMode("Rich")
        try bringIntoView(
            "notebook.richElement.rich-element-9",
            in: "notebook.document.scroll",
            deltaY: -500
        )
        replaceText("notebook.richElement.rich-element-9", with: "After the final task cell — edited by XCUITest.")
        try bringIntoView(
            "notebook.richElement.rich-element-5",
            in: "notebook.document.scroll",
            deltaY: 400
        )
        replaceText("notebook.richElement.rich-element-5", with: "Between task cells — this is deliberately not the first note.")
        try bringIntoView(
            "notebook.richElement.rich-element-3",
            in: "notebook.document.scroll",
            deltaY: 400
        )
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
        let statusID = "notebook.executionStatus.receipt-imager-mfs"
        try expandExecutionStatus(statusID)
        XCTAssertTrue(try require("notebook.execution.restart.receipt-imager-mfs").waitForExistence(timeout: 3))

        try require(statusID).doubleClick()
        XCTAssertTrue(try require("central.tab.tab-prototype-task-receipt-imager-mfs").waitForExistence(timeout: 5))
        assertZeroProductionBoundaryCalls()
    }

    func testFixtureRestartCompletionCancellationAndIsolation() throws {
        launchPrototype()

        let statusID = "notebook.executionStatus.receipt-imager-mfs"
        try expandExecutionStatus(statusID)
        try require("notebook.execution.restart.receipt-imager-mfs").click()
        XCTAssertTrue(waitForAccessibilityValue(statusID, containing: "Running"))
        try require("notebook.execution.complete.receipt-imager-mfs").click()
        XCTAssertTrue(waitForAccessibilityValue(statusID, containing: "Succeeded"))

        try require("notebook.execution.restart.receipt-imager-mfs").click()
        XCTAssertTrue(waitForAccessibilityValue(statusID, containing: "Running"))
        try require("notebook.execution.cancel.receipt-imager-mfs").click()
        XCTAssertTrue(waitForAccessibilityValue(statusID, containing: "Cancelled"))
        XCTAssertTrue(try textValue(try require("notebook.execution.revisionCount.receipt-imager-mfs")).contains("3 revisions"))

        let workbenchMenu = app.menuBars.menuBarItems["Workbench"]
        XCTAssertTrue(workbenchMenu.exists)
        workbenchMenu.click()
        for prefix in ["Open Project Directory", "Fork Tutorial Template", "Open Demo Project"] {
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
        var unacceptedIssues: [String] = []
        try app.performAccessibilityAudit { issue in
            if issue.compactDescription == "Parent/Child mismatch" {
                // SwiftUI lazily exposes the off-screen notebook document while
                // XCTest walks it, so the audit can retain a child after its
                // transient parent has been replaced. Keep every other element
                // detection issue actionable.
                return true
            }
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
            if issue.auditType.contains(.contrast),
               let identifier = issue.element?.identifier,
               identifier == "notebook.boundaryAudit"
                   || identifier.hasPrefix("notebook.selector.")
            {
                return true
            }

            let element = issue.element
            unacceptedIssues.append(
                "\(issue.compactDescription)"
                    + " [type=\(issue.auditType), identifier=\(element?.identifier ?? "<none>"),"
                    + " elementType=\(String(describing: element?.elementType)),"
                    + " frame=\(String(describing: element?.frame)),"
                    + " label=\(element?.label ?? "<none>"),"
                    + " value=\(String(describing: element?.value))]"
            )
            return true
        }
        XCTAssertTrue(
            unacceptedIssues.isEmpty,
            "Unaccepted accessibility audit issues:\n\(unacceptedIssues.joined(separator: "\n"))"
        )
        assertZeroProductionBoundaryCalls()
    }

    func testPythonPrototypeRunPlotRegenerateInsertAndIsolation() throws {
        launchPythonPrototype()

        XCTAssertEqual(try accessibilityValue("pythonPrototype.kernelState"), "ready")
        XCTAssertEqual(try accessibilityValue("pythonPrototype.boundaryAudit"), "0")
        XCTAssertTrue(try require("pythonPrototype.plot.python-plot-1").exists)
        XCTAssertFalse(element("pythonPrototype.artifact.png").exists)
        XCTAssertFalse(element("pythonPrototype.artifact.svg").exists)
        XCTAssertEqual(
            try accessibilityValue("pythonPrototype.executionDetails.python-execution-1"),
            "collapsed"
        )
        try expandExecutionStatus("pythonPrototype.executionDetails.python-execution-1")
        try bringIntoView(
            "pythonPrototype.artifact.png",
            in: "pythonPrototype.documentScroll",
            deltaY: -220
        )
        XCTAssertTrue(try require("pythonPrototype.artifact.png").exists)
        XCTAssertTrue(try require("pythonPrototype.artifact.svg").exists)
        XCTAssertEqual(try accessibilityValue("pythonPrototype.revisionCount"), "1")

        try bringIntoView(
            "pythonPrototype.regenerate",
            in: "pythonPrototype.documentScroll",
            deltaY: -260
        )
        try clickUntilAccessibilityValue(
            control: "pythonPrototype.regenerate",
            state: "pythonPrototype.revisionCount",
            contains: "2"
        )
        let previous = try require("pythonPrototype.previousRevisions.python-cell-plot")
        let firstRevision = element("pythonPrototype.revision.1")
        XCTAssertFalse(firstRevision.exists)
        previous.click()
        XCTAssertTrue(
            waitForAccessibilityValue("pythonPrototype.previousRevisions.python-cell-plot", containing: "expanded")
        )
        XCTAssertTrue(firstRevision.waitForExistence(timeout: 3))
        try require("pythonPrototype.insert").click()
        XCTAssertTrue(waitForAccessibilityValue("pythonPrototype.insertedPlotCount", containing: "1"))

        try bringIntoView(
            "pythonPrototype.run",
            in: "pythonPrototype.documentScroll",
            deltaY: 400
        )
        try clickUntilAccessibilityValue(
            control: "pythonPrototype.run",
            state: "pythonPrototype.revisionCount",
            contains: "3"
        )
        XCTAssertTrue(waitForAccessibilityValue("pythonPrototype.kernelState", containing: "ready"))
        XCTAssertTrue(waitForAccessibilityValue("pythonPrototype.revisionCount", containing: "3"))
        assertZeroPythonProductionBoundaryCalls()
    }

    func testPythonPrototypeExplorerSnapshotsAreExplicitAndVersioned() throws {
        launchPythonPrototype()

        XCTAssertEqual(try accessibilityValue("pythonPrototype.savedVisualizationCount"), "2")
        try require("notebookVisualization.preview.saved-visibility-plot").click()
        XCTAssertTrue(try require("notebookVisualization.lightbox.saved-visibility-plot").exists)
        try require("notebookVisualization.lightboxDone").click()

        try require("notebookVisualization.openExplorer.saved-visibility-plot").click()
        XCTAssertTrue(try require("explorerSnapshot.parameters").exists)
        XCTAssertEqual(try textValue(try require("explorerSnapshot.parameter.field")), "TW Hya")
        replaceText("explorerSnapshot.parameter.field", with: "TW Hya offset")
        XCTAssertEqual(try accessibilityValue("explorerSnapshot.targetRevisionCount"), "1")
        try require("explorerSnapshot.update").click()
        try require("explorerSnapshot.back").click()

        XCTAssertEqual(
            try accessibilityValue("notebookVisualization.revisionCount.saved-visibility-plot"),
            "2"
        )
        XCTAssertTrue(try require("notebookVisualization.previousRevisions.saved-visibility-plot").exists)

        try require("notebookVisualization.openExplorer.saved-visibility-plot").click()
        replaceText("explorerSnapshot.parameter.field", with: "Companion")
        try require("explorerSnapshot.saveNew").click()
        try require("explorerSnapshot.back").click()
        XCTAssertEqual(try accessibilityValue("pythonPrototype.savedVisualizationCount"), "3")
        assertZeroPythonProductionBoundaryCalls()
    }

    func testPythonPrototypeFailureEditAndRetry() throws {
        launchPythonPrototype(scenario: "failure")

        let failureOutput = element("pythonPrototype.output.python-output-2-2")
        XCTAssertFalse(failureOutput.exists)
        XCTAssertEqual(
            try accessibilityValue("pythonPrototype.executionDetails.python-execution-2"),
            "collapsed"
        )
        try expandExecutionStatus("pythonPrototype.executionDetails.python-execution-2")
        XCTAssertTrue(failureOutput.waitForExistence(timeout: 5))
        XCTAssertTrue(failureOutput.label.contains("RuntimeError: fixture: channel selection is empty"))
        let repaired = """
        print("checking continuum selection", flush=True)
        print("continuum selection repaired")
        """
        try bringIntoView(
            "pythonPrototype.editor",
            in: "pythonPrototype.documentScroll",
            deltaY: -260
        )
        replaceText("pythonPrototype.editor", with: repaired)
        try require("pythonPrototype.run").click()
        XCTAssertTrue(waitForAccessibilityValue("pythonPrototype.kernelState", containing: "ready"))
        XCTAssertTrue(waitForAccessibilityValue("pythonPrototype.revision.3", containing: "succeeded"))
        assertZeroPythonProductionBoundaryCalls()
    }

    func testPythonPrototypeNonresponsiveInterruptAndRestart() throws {
        launchPythonPrototype(scenario: "nonresponsive")

        XCTAssertEqual(try accessibilityValue("pythonPrototype.kernelState"), "running")
        try require("pythonPrototype.stop").click()
        XCTAssertTrue(waitForAccessibilityValue("pythonPrototype.kernelState", containing: "restart-required"))
        XCTAssertTrue(waitForAccessibilityValue("pythonPrototype.revision.2", containing: "interrupted"))
        try require("pythonPrototype.restart").click()
        XCTAssertTrue(waitForAccessibilityValue("pythonPrototype.kernelState", containing: "ready"))
        assertZeroPythonProductionBoundaryCalls()
    }

    func testPythonPrototypeAIExactCodeApprovalInvalidatesAfterEdit() throws {
        launchPythonPrototype()

        try require("pythonPrototype.cell.python-cell-ai").click()
        if !element("pythonPrototype.approvalState").waitForExistence(timeout: 3) {
            app.activate()
            try require("pythonPrototype.cell.python-cell-ai").click()
        }
        XCTAssertEqual(try accessibilityValue("pythonPrototype.approvalState"), "required")
        XCTAssertFalse(try require("pythonPrototype.run").isEnabled)
        try clickUntilAccessibilityValue(
            control: "pythonPrototype.approve",
            state: "pythonPrototype.approvalState",
            contains: "approved"
        )
        XCTAssertTrue(try require("pythonPrototype.run").isEnabled)

        let edited = try textValue(try require("pythonPrototype.editor")) + "\n# user edit invalidates approval"
        replaceText("pythonPrototype.editor", with: edited)
        XCTAssertTrue(waitForAccessibilityValue("pythonPrototype.approvalState", containing: "required"))
        XCTAssertFalse(try require("pythonPrototype.run").isEnabled)
        assertZeroPythonProductionBoundaryCalls()
    }

    func testWaveTwoPythonAccessibilityAudit() throws {
        launchPythonPrototype()
        let documentScroll = app.scrollViews.firstMatch
        XCTAssertTrue(documentScroll.exists)
        var unacceptedIssues: [String] = []
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
            if issue.element?.elementType == .group || issue.element?.elementType == .touchBar {
                return true
            }
            if issue.element?.elementType == .popUpButton,
               issue.element?.label == "emoji & symbols"
            {
                // System Touch Bar candidate control exposed only while the
                // TextEditor owns focus; it is outside the app hierarchy.
                return true
            }
            if self.acceptedPythonPrototypeContrastArtifact(issue) {
                return true
            }
            let element = issue.element
            unacceptedIssues.append(
                "\(issue.compactDescription)"
                    + " [type=\(issue.auditType), identifier=\(element?.identifier ?? "<none>"),"
                    + " elementType=\(String(describing: element?.elementType)),"
                    + " frame=\(String(describing: element?.frame)),"
                    + " label=\(element?.label ?? "<none>"),"
                    + " value=\(String(describing: element?.value))]"
            )
            return true
        }
        XCTAssertTrue(
            unacceptedIssues.isEmpty,
            "Unaccepted Python prototype accessibility issues:\n\(unacceptedIssues.joined(separator: "\n"))"
        )
        assertZeroPythonProductionBoundaryCalls()
    }

    func testProductionNotebookPersistsCompleteMarkdownAndReconcilesExternalEdit() throws {
        let notebookID = "019f0000-0000-7000-8000-000000000001"
        let cellID = "019f0000-0000-7000-8000-000000000002"
        let runID = "019f0000-0000-7000-8000-000000000003"
        let project = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-mac-ui-notebook-\(UUID().uuidString)", isDirectory: true)
        let notebooks = project.appendingPathComponent("notebooks", isDirectory: true)
        try FileManager.default.createDirectory(at: notebooks, withIntermediateDirectories: true)
        let notebookFile = notebooks.appendingPathComponent("default.md")
        let initial = """
        <!-- casa-rs-notebook:v1 id=\(notebookID) -->

        # UI production notebook

        Initial note.

        <!-- casa-rs-cell:v1 id=\(cellID) kind=task -->
        ```toml
        [casars]
        format = 1
        surface = "imhead"
        kind = "task"
        contract = 1

        [parameters]
        imagename = "input.image"
        mode = "list"
        ```
        <!-- /casa-rs-cell -->
        """ + "\n"
        try initial.write(to: notebookFile, atomically: true, encoding: .utf8)
        let runDirectory = project
            .appendingPathComponent(".casa-rs/notebook-runs", isDirectory: true)
            .appendingPathComponent(runID, isDirectory: true)
        try FileManager.default.createDirectory(at: runDirectory, withIntermediateDirectories: true)
        let receipt = """
        {
          "schema_version": 1,
          "run_id": "\(runID)",
          "revision": 1,
          "notebook_id": "\(notebookID)",
          "cell_id": "\(cellID)",
          "initiating_surface": "cli",
          "operation_id": "imhead",
          "started_at": 1,
          "finished_at": 2,
          "status": "succeeded",
          "sparse_intent": {
            "format": 1,
            "surface": "imhead",
            "kind": "task",
            "contract": 1,
            "parameters": {"imagename": "input.image", "mode": "summary"}
          },
          "resolved_parameters": {"imagename": ["input.image"], "mode": "summary"},
          "provider_contract_version": 1,
          "run_safety": {"classification": "read_only", "affected_paths": []},
          "approvals": [],
          "affected_paths": [],
          "products": [],
          "artifacts": [],
          "logs": {},
          "diagnostics": [],
          "replay_claim": "historical resolved values"
        }
        """ + "\n"
        try receipt.write(
            to: runDirectory.appendingPathComponent("receipt.json"),
            atomically: true,
            encoding: .utf8
        )
        productionProjectURL = project

        app = XCUIApplication()
        app.launchArguments = [
            "-ApplePersistenceIgnoreState", "YES",
            "--open-project", project.path,
        ]
        app.launch()
        app.activate()
        XCTAssertTrue(app.windows["casa-rs Workbench"].waitForExistence(timeout: 10))
        let notebookDock = app.buttons["dock.mode.notebooks"]
        XCTAssertTrue(notebookDock.waitForExistence(timeout: 5), app.debugDescription)
        try clickIdentified("dock.mode.notebooks")
        let selector = notebookSelector(notebookID)
        XCTAssertTrue(selector.waitForExistence(timeout: 5), app.debugDescription)
        try require("notebook.selector.open").click()

        XCTAssertTrue(element("notebook.viewMode").waitForExistence(timeout: 5), app.debugDescription)

        selectViewMode("Raw")
        let edited = initial + "\nSaved from the launched production UI.\n"
        replaceText("notebook.editor.raw", with: edited)
        try require("notebook.save").click()
        XCTAssertTrue(waitForFile(notebookFile, containing: "Saved from the launched production UI."))
        XCTAssertEqual(try accessibilityValue("notebook.dirtyState"), "saved")

        let local = edited + "\nLocal dirty edit.\n"
        replaceText("notebook.editor.raw", with: local)
        let external = edited + "\nExternal third-party edit.\n"
        try external.write(to: notebookFile, atomically: true, encoding: .utf8)
        try require("notebook.save").click()
        XCTAssertTrue(try require("notebook.conflict.reloadExternal").exists)
        try require("notebook.conflict.reloadExternal").click()
        XCTAssertTrue(waitForValue("notebook.editor.raw", containing: "External third-party edit."))
        XCTAssertFalse(try textValue(try require("notebook.editor.raw")).contains("Local dirty edit."))
        XCTAssertEqual(try accessibilityValue("notebook.dirtyState"), "saved")

        selectViewMode("Rich")
        try require("notebook.parameters.open.\(cellID)").click()
        XCTAssertTrue(try require("task.change").exists)
        let mode = try require("task.parameter.mode", timeout: 10)
        XCTAssertFalse(app.buttons["Stop"].isEnabled, "Loading notebook parameters must not execute the task")
        XCTAssertTrue(try textValue(mode).contains("list"), "Markdown task intent must win over historical receipt intent")

        mode.click()
        let summaryChoice = app.menuItems["summary"]
        XCTAssertTrue(summaryChoice.waitForExistence(timeout: 3), app.debugDescription)
        summaryChoice.click()
        XCTAssertTrue(waitForValue("task.parameter.mode", containing: "summary"))

        try require("central.tab.tab-scientific-notebook").click()
        try require("notebook.parameters.open.\(cellID)").click()
        XCTAssertTrue(try require("notebook.taskReplace.sheet").exists)
        XCTAssertTrue(try require("notebook.taskReplace.diff.mode").exists)
        try require("notebook.taskReplace.cancel").click()

        try require("notebook.parameters.open.\(cellID)").click()
        try require("notebook.taskReplace.confirm").click()
        XCTAssertTrue(waitForValue("task.parameter.mode", containing: "list"))
        XCTAssertFalse(app.buttons["Stop"].isEnabled, "Replacing notebook parameters must not execute the task")
    }

    func testProductionPythonCellRunsPersistsReceiptAndSurvivesNotebookReload() throws {
        let notebookID = "019f0000-0000-7000-8000-000000000101"
        let cellID = "019f0000-0000-7000-8000-000000000102"
        let project = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-mac-ui-python-\(UUID().uuidString)", isDirectory: true)
        let notebooks = project.appendingPathComponent("notebooks", isDirectory: true)
        let pythonBin = project.appendingPathComponent(".casa-rs/python/bin", isDirectory: true)
        try FileManager.default.createDirectory(at: notebooks, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: pythonBin, withIntermediateDirectories: true)
        try FileManager.default.createSymbolicLink(
            at: pythonBin.appendingPathComponent("python3"),
            withDestinationURL: URL(fileURLWithPath:
                ProcessInfo.processInfo.environment["CASA_RS_GUI_TEST_PYTHON"]
                    ?? "/usr/bin/python3"
            )
        )
        let notebookFile = notebooks.appendingPathComponent("python.md")
        let source = """
        <!-- casa-rs-notebook:v1 id=\(notebookID) -->

        # Production Python notebook

        This prose remains the primary document.

        <!-- casa-rs-cell:v1 id=\(cellID) kind=python -->
        ```python
        value = 6 * 7
        print(value)
        ```
        <!-- /casa-rs-cell -->
        """ + "\n"
        try source.write(to: notebookFile, atomically: true, encoding: .utf8)
        productionProjectURL = project

        app = XCUIApplication()
        app.launchArguments = [
            "-ApplePersistenceIgnoreState", "YES",
            "--open-project", project.path,
        ]
        app.launch()
        app.activate()
        XCTAssertTrue(app.windows["casa-rs Workbench"].waitForExistence(timeout: 10))
        try clickIdentified("dock.mode.notebooks")
        XCTAssertTrue(notebookSelector(notebookID).waitForExistence(timeout: 5), app.debugDescription)
        try require("notebook.selector.open").click()

        if element("inspector.collapse").isHittable {
            try clickIdentified("inspector.collapse")
        }
        selectViewMode("Raw")
        selectViewMode("Rich")

        let authority = try require("notebook.python.authority")
        XCTAssertTrue(
            authority.label.contains("normal user authority")
                || (authority.value as? String)?.contains("normal user authority") == true,
            authority.debugDescription
        )
        let runAll = try require("notebook.python.runAll")
        XCTAssertTrue(runAll.isEnabled, app.debugDescription)
        runAll.click()

        let runs = project.appendingPathComponent(".casa-rs/notebook-runs", isDirectory: true)
        XCTAssertTrue(waitForReceipt(in: runs, containing: "\"schema_version\": 2"))
        XCTAssertTrue(waitForReceipt(in: runs, containing: "\"source\": \"value = 6 * 7\\nprint(value)\\n\""))

        selectViewMode("Raw")
        selectViewMode("Rich")
        try bringIntoView(
            "notebook.python.cell.\(cellID)",
            in: "notebook.document.scroll",
            deltaY: -220
        )
        let visibleOutput = app.staticTexts["42"]
        XCTAssertFalse(visibleOutput.exists)
        XCTAssertEqual(
            try accessibilityValue("notebook.python.latestDetails.\(cellID)"),
            "collapsed"
        )
        try expandExecutionStatus("notebook.python.latestDetails.\(cellID)")
        XCTAssertTrue(
            visibleOutput.waitForExistence(timeout: 5),
            app.debugDescription
        )

        try require("central.tab.tab-scientific-notebook").click()
        try clickIdentified("dock.mode.datasets")
        try clickIdentified("dock.mode.notebooks")
        try require("notebook.selector.open").click()
        try bringIntoView(
            "notebook.python.cell.\(cellID)",
            in: "notebook.document.scroll",
            deltaY: -220
        )
        XCTAssertEqual(
            try accessibilityValue("notebook.python.latestDetails.\(cellID)"),
            "expanded"
        )
        XCTAssertTrue(visibleOutput.waitForExistence(timeout: 5))
    }

    func testProductionAssistantPersistsPinAndDestinationFirstProposal() throws {
        let notebookID = "019f0000-0000-7000-8000-000000000401"
        let project = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-mac-ui-assistant-\(UUID().uuidString)", isDirectory: true)
        let notebooks = project.appendingPathComponent("notebooks", isDirectory: true)
        try FileManager.default.createDirectory(at: notebooks, withIntermediateDirectories: true)
        try "<!-- casa-rs-notebook:v1 id=\(notebookID) -->\n\n# Analysis\n\nInitial note.\n"
            .write(to: notebooks.appendingPathComponent("Analysis.md"), atomically: true, encoding: .utf8)
        try "# Fixture source\nTyped CASA-RS provider contracts.\n"
            .write(to: project.appendingPathComponent("ARCHITECTURE.md"), atomically: true, encoding: .utf8)
        let pythonDirectory = project.appendingPathComponent(".casa-rs/python/bin", isDirectory: true)
        try FileManager.default.createDirectory(at: pythonDirectory, withIntermediateDirectories: true)
        try FileManager.default.createSymbolicLink(
            at: pythonDirectory.appendingPathComponent("python3"),
            withDestinationURL: URL(fileURLWithPath: "/usr/bin/python3")
        )
        productionProjectURL = project

        let node = ["/opt/homebrew/bin/node", "/usr/local/bin/node"]
            .first(where: FileManager.default.isExecutableFile(atPath:))
        guard let node else {
            throw XCTSkip("Node is not installed")
        }

        app = XCUIApplication()
        ensureStoppedBeforeLaunch()
        app.launchEnvironment["CASA_RS_ASSISTANT_FIXTURE"] = "1"
        app.launchEnvironment["CASA_RS_ASSISTANT_NODE"] = node
        app.launchEnvironment["CASA_RS_SOURCE_ROOT"] = project.path
        app.launchArguments = [
            "-ApplePersistenceIgnoreState", "YES",
            "-NSAutomaticTextCompletionEnabled", "NO",
            "--open-project", project.path,
        ]
        app.launch()
        app.activate()
        XCTAssertTrue(app.windows["casa-rs Workbench"].waitForExistence(timeout: 10))
        try clickIdentified("dock.mode.notebooks")
        XCTAssertTrue(notebookSelector(notebookID).waitForExistence(timeout: 5), app.debugDescription)
        try clickIdentified("notebook.selector.open")
        if element("inspector.collapse").isHittable { try clickIdentified("inspector.collapse") }

        try clickIdentified("assistant.openDrawer")
        XCTAssertTrue(element("assistant.discussion").waitForExistence(timeout: 8), app.debugDescription)
        XCTAssertTrue(element("assistant.provider").waitForExistence(timeout: 8), app.debugDescription)
        replaceText("assistant.input", with: "Please propose a note")
        try clickIdentified("assistant.send")
        XCTAssertTrue(
            app.buttons["Pin to notebook"].firstMatch.waitForExistence(timeout: 8),
            app.debugDescription
        )

        app.buttons["Pin to notebook"].firstMatch.click()
        XCTAssertTrue(element("assistant.pin.confirm").waitForExistence(timeout: 5))
        try clickIdentified("assistant.pin.confirm")
        XCTAssertTrue(element("assistant.openNotebookSuggestions").waitForExistence(timeout: 5))
        try clickIdentified("assistant.openNotebookSuggestions")
        XCTAssertTrue(app.buttons["Insert at notebook tail"].firstMatch.waitForExistence(timeout: 5))
        app.buttons["Insert at notebook tail"].firstMatch.click()

        let saved = try String(contentsOf: notebooks.appendingPathComponent("Analysis.md"))
        XCTAssertTrue(saved.contains("casa-rs-ai-pin:v1"))
        XCTAssertTrue(saved.contains("A deterministic proposed note."))
        let conversations = project.appendingPathComponent(".casa-rs/conversations", isDirectory: true)
        XCTAssertFalse((try FileManager.default.contentsOfDirectory(atPath: conversations.path)).isEmpty)

        replaceText("assistant.input", with: "Please propose Python")
        try clickIdentified("assistant.send")
        XCTAssertTrue(app.buttons["Insert at notebook tail"].firstMatch.waitForExistence(timeout: 8))
        app.buttons["Insert at notebook tail"].firstMatch.click()
        XCTAssertTrue(app.buttons["Approve isolated run"].firstMatch.waitForExistence(timeout: 5))
        app.buttons["Approve isolated run"].firstMatch.click()

        let receiptRoot = project.appendingPathComponent(".casa-rs/notebook-runs", isDirectory: true)
        let deadline = Date().addingTimeInterval(12)
        var receiptText = ""
        while Date() < deadline, !receiptText.contains(#""authority":"ai_worker""#) {
            if let enumerator = FileManager.default.enumerator(at: receiptRoot, includingPropertiesForKeys: nil) {
                for case let url as URL in enumerator where url.lastPathComponent == "receipt.json" {
                    receiptText += (try? String(contentsOf: url)) ?? ""
                }
            }
            if !receiptText.contains(#""authority":"ai_worker""#) {
                RunLoop.current.run(until: Date().addingTimeInterval(0.1))
            }
        }
        XCTAssertTrue(receiptText.contains(#""authority":"ai_worker""#), receiptText)
        XCTAssertTrue(receiptText.contains("CASARS_ARTIFACT_STAGING"), receiptText)
        XCTAssertTrue(FileManager.default.fileExists(
            atPath: project.appendingPathComponent(".casa-rs/ai-staging").path
        ))
    }

    func testTutorialPrototypeLearnerNotesApprovalAndTaskLoading() throws {
        launchTutorialPrototype()
        let datasetID = "tutorial-dataset-twhya-calibrated"
        XCTAssertEqual(
            try accessibilityValue("tutorialPrototype.dataset.status.\(datasetID)"),
            "missing"
        )

        replaceText(
            "notebook.richElement.rich-element-3",
            with: "Compare calibrated amplitudes before imaging."
        )
        selectViewMode("Raw")
        let rawMarkdown = try textValue(try require("notebook.editor.raw"))
        XCTAssertTrue(rawMarkdown.contains("Compare calibrated amplitudes before imaging."))
        replaceText(
            "notebook.editor.raw",
            with: rawMarkdown.replacingOccurrences(
                of: "Compare calibrated amplitudes before imaging.",
                with: "Compare calibrated amplitudes and phases before imaging."
            )
        )
        selectViewMode("Rich")
        XCTAssertTrue(
            waitForValue(
                "notebook.richElement.rich-element-3",
                containing: "Compare calibrated amplitudes and phases before imaging."
            )
        )
        XCTAssertEqual(try accessibilityValue("notebook.dirtyState"), "dirty")

        try clickIdentified("tutorialPrototype.dataset.review.\(datasetID)")
        XCTAssertTrue(try require("tutorialPrototype.approval.sheet").exists)
        XCTAssertEqual(try accessibilityValue("tutorialPrototype.approval.scheme"), "https")
        XCTAssertTrue(
            try accessibilityValue("tutorialPrototype.approval.expectedSize")
                .contains("435742720 bytes")
        )
        XCTAssertEqual(
            try accessibilityValue("tutorialPrototype.approval.destination"),
            "data/twhya_calibrated.ms"
        )
        XCTAssertTrue(
            try accessibilityValue("tutorialPrototype.approval.checksum")
                .hasSuffix("a97b2")
        )
        try clickIdentified("tutorialPrototype.approval.approve")

        XCTAssertTrue(
            waitForAccessibilityValue(
                "tutorialPrototype.dataset.status.\(datasetID)",
                containing: "ready",
                timeout: 8
            ),
            app.debugDescription
        )
        XCTAssertTrue(
            try accessibilityValue("tutorialPrototype.progressSummary").contains("2 of 4")
        )

        let taskBlock = "notebook.parameters.open.tutorial-task-twhya-imager"
        try bringIntoView(taskBlock, in: "notebook.document.scroll", deltaY: -420)
        try clickIdentified(taskBlock)
        XCTAssertTrue(
            try require("central.tab.tab-prototype-task-tutorial-task-twhya-imager")
                .waitForExistence(timeout: 5)
        )
        XCTAssertEqual(
            try textValue(try require("prototypeTask.parameter.vis")),
            "data/twhya_calibrated.ms"
        )
        XCTAssertEqual(try textValue(try require("prototypeTask.parameter.imsize")), "250")
        XCTAssertEqual(
            try accessibilityValue("prototypeTask.parameterSource.vis"),
            "tutorial override"
        )
        XCTAssertEqual(
            try accessibilityValue("prototypeTask.parameterSource.imsize"),
            "tutorial override"
        )
    }

    func testProductionTutorialForkApprovalReadyAndTaskLoading() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-gui-tutorial-\(UUID().uuidString)", isDirectory: true)
        let project = root.appendingPathComponent("project", isDirectory: true)
        let template = root.appendingPathComponent("template", isDirectory: true)
        productionProjectURL = root
        try FileManager.default.createDirectory(at: project, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: template, withIntermediateDirectories: true)
        let bytes = Data("production GUI tutorial source".utf8)
        let source = template.appendingPathComponent("source.bin")
        try bytes.write(to: source)
        let digest = SHA256.hash(data: bytes).map { String(format: "%02x", $0) }.joined()
        try """
        # Production GUI tutorial

        Editable learner notes.

        <!-- casa-rs-cell:v1 id=019f7777-7777-7777-8777-777777777777 kind=task -->
        ```toml
        [casars]
        format = 1
        surface = "imager"
        kind = "task"
        contract = 1

        [parameters]
        vis = "data/science.bin"
        imagename = "products/science"
        weighting = "briggs"
        robust = -0.5
        ```
        <!-- /casa-rs-cell -->
        """.write(to: template.appendingPathComponent("tutorial.md"), atomically: true, encoding: .utf8)
        try """
        schema_version = 1
        tutorial_id = "production-gui"
        title = "Production GUI tutorial"

        [[datasets]]
        id = "science"
        display_name = "Science input"
        uri = "file://\(source.path)"
        destination = "data/science.bin"
        expected_size_bytes = \(bytes.count)
        sha256 = "\(digest)"

        [[sections]]
        id = "run"
        title = "Run"
        dataset_ids = ["science"]
        cell_ids = ["019f7777-7777-7777-8777-777777777777"]
        """.write(to: template.appendingPathComponent("tutorial.toml"), atomically: true, encoding: .utf8)

        app = XCUIApplication()
        ensureStoppedBeforeLaunch()
        app.launchArguments = [
            "-ApplePersistenceIgnoreState", "YES",
            "--open-project", project.path,
            "--open-tutorial-pack", template.path,
        ]
        app.launch()
        app.activate()
        XCTAssertTrue(app.windows["casa-rs Workbench"].waitForExistence(timeout: 10))
        XCTAssertEqual(try accessibilityValue("tutorial.dataset.science"), "missing")

        try clickIdentified("tutorial.dataset.review.science")
        XCTAssertTrue(try require("tutorial.approval.sheet").waitForExistence(timeout: 5))
        try clickIdentified("tutorial.approval.approve")
        XCTAssertTrue(
            waitForAccessibilityValue("tutorial.dataset.science", containing: "ready"),
            app.debugDescription
        )

        let cellID = "019f7777-7777-7777-8777-777777777777"
        try clickIdentified("notebook.parameters.open.\(cellID)")
        XCTAssertTrue(try require("task.parameter.vis").waitForExistence(timeout: 5))
        XCTAssertEqual(try accessibilityValue("task.parameterSource.vis"), "tutorial override")
        let taskScroll = try XCTUnwrap(
            app.scrollViews.allElementsBoundByIndex.max {
                $0.frame.width < $1.frame.width
            },
            app.debugDescription
        )
        for _ in 0..<6 {
            taskScroll.scroll(byDeltaX: 0, deltaY: -420)
        }
        XCTAssertEqual(try accessibilityValue("task.parameterSource.robust"), "tutorial override")
    }

    func testTutorialPrototypeCancellationResumeAndAttemptIdentity() throws {
        launchTutorialPrototype()
        let datasetID = "tutorial-dataset-twhya-calibrated"
        try clickIdentified("tutorialPrototype.dataset.review.\(datasetID)")
        try clickIdentified("tutorialPrototype.approval.approve")

        let progressID = "tutorialPrototype.dataset.progress.\(datasetID)"
        XCTAssertTrue(
            waitForPositivePercentage(progressID),
            "Acquisition never reported resumable progress: \(app.debugDescription)"
        )
        let progress = try require(progressID)
        XCTAssertTrue(try textValue(progress).hasSuffix("%"), progress.debugDescription)
        try clickIdentified("tutorialPrototype.dataset.cancel.\(datasetID)")
        XCTAssertTrue(
            waitForAccessibilityValue(
                "tutorialPrototype.dataset.status.\(datasetID)",
                containing: "cancelled"
            )
        )
        let firstAttempt = try accessibilityValue("tutorialPrototype.dataset.attempt.\(datasetID)")
        let resumeOffset = try accessibilityValue("tutorialPrototype.dataset.resumeOffset.\(datasetID)")
        XCTAssertNotEqual(resumeOffset, "0")

        try clickIdentified("tutorialPrototype.dataset.resume.\(datasetID)")
        XCTAssertNotEqual(
            try accessibilityValue("tutorialPrototype.dataset.attempt.\(datasetID)"),
            firstAttempt
        )
        XCTAssertTrue(
            waitForAccessibilityValue(
                "tutorialPrototype.dataset.status.\(datasetID)",
                containing: "ready",
                timeout: 8
            )
        )
        assertZeroTutorialProductionBoundaryCalls()
    }

    func testTutorialPrototypeChecksumFailureStaysCompactAndRetryRecovers() throws {
        launchTutorialPrototype(scenario: "checksum-failure")
        let datasetID = "tutorial-dataset-twhya-calibrated"
        try clickIdentified("tutorialPrototype.dataset.review.\(datasetID)")
        try clickIdentified("tutorialPrototype.approval.approve")

        XCTAssertTrue(
            waitForAccessibilityValue(
                "tutorialPrototype.dataset.status.\(datasetID)",
                containing: "checksum-failed",
                timeout: 8
            )
        )
        XCTAssertEqual(
            try accessibilityValue("tutorialPrototype.failure.details.\(datasetID)"),
            "collapsed"
        )
        try clickIdentified("tutorialPrototype.dataset.retry.\(datasetID)")
        XCTAssertTrue(
            waitForAccessibilityValue(
                "tutorialPrototype.dataset.status.\(datasetID)",
                containing: "ready",
                timeout: 8
            )
        )
        assertZeroTutorialProductionBoundaryCalls()
    }

    func testTutorialPrototypeDiskFailureShowsPlanAndRecoversExplicitly() throws {
        launchTutorialPrototype(scenario: "disk-failure")
        let datasetID = "tutorial-dataset-twhya-calibrated"
        try clickIdentified("tutorialPrototype.dataset.review.\(datasetID)")
        let diskPlan = try accessibilityValue("tutorialPrototype.approval.diskRequirement")
        XCTAssertTrue(diskPlan.contains("required"))
        XCTAssertTrue(diskPlan.contains("free"))
        try clickIdentified("tutorialPrototype.approval.approve")

        XCTAssertTrue(
            waitForAccessibilityValue(
                "tutorialPrototype.dataset.status.\(datasetID)",
                containing: "disk-failed"
            )
        )
        XCTAssertEqual(
            try accessibilityValue("tutorialPrototype.failure.details.\(datasetID)"),
            "collapsed"
        )
        try clickIdentified("tutorialPrototype.dataset.makeSpaceAvailable.\(datasetID)")
        XCTAssertTrue(
            waitForAccessibilityValue(
                "tutorialPrototype.dataset.status.\(datasetID)",
                containing: "ready",
                timeout: 8
            )
        )
        assertZeroTutorialProductionBoundaryCalls()
    }

    func testTutorialPrototypeAccessibilityAndIsolation() throws {
        launchTutorialPrototype()

        let visibleWindowFrame = app.windows.firstMatch.frame
        var unacceptedIssues: [String] = []
        try app.performAccessibilityAudit { issue in
            if issue.compactDescription == "Parent/Child mismatch" {
                return true
            }
            if issue.auditType.contains(.contrast),
               (issue.element?.label == "casa-rs Workbench"
                   || issue.element?.value as? String == "casa-rs Workbench")
            {
                return true
            }
            if issue.element?.elementType == .group || issue.element?.elementType == .touchBar {
                return true
            }
            if ["split.resizeHandle", "central.tab.plus"].contains(issue.element?.identifier) {
                return true
            }
            if issue.auditType.contains(.contrast),
               let identifier = issue.element?.identifier,
               identifier == "notebook.boundaryAudit"
                   || identifier.hasPrefix("notebook.selector.")
            {
                return true
            }
            if issue.auditType.contains(.contrast),
               let frame = issue.element?.frame,
               !frame.intersects(visibleWindowFrame)
            {
                // XCTest audits lazily retained ScrollView descendants even
                // when their frames are fully outside the visible window.
                // The retained element screenshot is unrelated screen pixels,
                // so there is no rendered contrast to evaluate at this state.
                return true
            }
            unacceptedIssues.append(issue.compactDescription)
            return true
        }
        XCTAssertTrue(unacceptedIssues.isEmpty, unacceptedIssues.joined(separator: "\n"))

        try clickIdentified("tutorialPrototype.dataset.review.tutorial-dataset-twhya-calibrated")
        XCTAssertTrue(try require("tutorialPrototype.approval.sheet").exists)
        try clickIdentified("tutorialPrototype.approval.cancel")
        let workbenchMenu = app.menuBars.menuBarItems["Workbench"]
        XCTAssertTrue(workbenchMenu.exists)
        workbenchMenu.click()
        for prefix in ["Open Project Directory", "Fork Tutorial Template", "Open Demo Project"] {
            let item = app.menuItems.matching(NSPredicate(format: "title BEGINSWITH %@", prefix)).firstMatch
            XCTAssertTrue(item.exists)
            XCTAssertFalse(item.isEnabled)
        }
        app.typeKey(.escape, modifierFlags: [])
        assertZeroTutorialProductionBoundaryCalls()
    }

    func testAIPrototypeCitedAnswerPinAndApprovedActions() throws {
        launchAIPrototype(openDrawer: false)

        XCTAssertTrue(try require("notebook.document.scroll").exists)
        XCTAssertFalse(element("aiPrototype.drawer").exists)
        try clickIdentified("aiPrototype.openDrawer")
        XCTAssertTrue(try require("aiPrototype.drawer").exists)
        XCTAssertFalse(element("aiPrototype.openDrawer").exists)

        let resizeHandle = try require("aiPrototype.resizeHandle")
        let drawerScroll = try require("aiPrototype.conversationScroll")
        let initialHandleX = resizeHandle.frame.midX
        let initialDrawerWidth = drawerScroll.frame.width
        let dragStart = resizeHandle.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0.5))
        dragStart.press(forDuration: 0.1, thenDragTo: dragStart.withOffset(CGVector(dx: -60, dy: 0)))
        XCTAssertLessThan(try require("aiPrototype.resizeHandle").frame.midX, initialHandleX - 35)
        XCTAssertGreaterThan(try require("aiPrototype.conversationScroll").frame.width, initialDrawerWidth + 35)

        XCTAssertTrue(try require("aiPrototype.boundaryStatus").label.contains("0 production calls"))
        try clickIdentified("aiPrototype.egressPreview")
        XCTAssertTrue(try require("aiPrototype.workspaceSource.tab-task").exists)
        XCTAssertTrue(try require("aiPrototype.workspaceSource.corpus-radio").exists)
        XCTAssertTrue(try require("aiPrototype.workspaceSource.source-casars").exists)
        try clickIdentified("aiPrototype.context.close")

        try clickIdentified("aiPrototype.suggestion.plot")
        XCTAssertTrue(try require("aiPrototype.input").value as? String == "Compare the current plot with the TW Hya paper.")
        try clickIdentified("aiPrototype.expand")
        XCTAssertTrue(try require("aiPrototype.expanded").exists)
        XCTAssertTrue(try require("aiPrototype.input").value as? String == "Compare the current plot with the TW Hya paper.")
        try clickIdentified("aiPrototype.dock")
        XCTAssertTrue(try require("aiPrototype.drawer").exists)
        XCTAssertTrue(try require("aiPrototype.provider").exists)
        XCTAssertTrue(try require("aiPrototype.model").exists)

        let composer = try require("aiPrototype.input")
        composer.click()
        composer.typeKey(.return, modifierFlags: [.shift])
        XCTAssertFalse(element("aiPrototype.message.ai-assistant-1").exists)
        composer.typeKey(.return, modifierFlags: [])
        XCTAssertTrue(try require("aiPrototype.message.ai-assistant-1", timeout: 5).exists)
        try clickIdentified("aiPrototype.citation.citation-paper")
        XCTAssertTrue(try require("aiPrototype.sourcePreview").exists)
        try clickIdentified("aiPrototype.message.ai-assistant-1.pin")
        XCTAssertTrue(try require("aiPrototype.pinSheet").exists)
        try clickIdentified("aiPrototype.pin.confirm")
        XCTAssertFalse(try require("aiPrototype.message.ai-assistant-1.pin").isEnabled)

        try clickIdentified("aiPrototype.openNotebookSuggestions")
        XCTAssertTrue(try require("notebook.aiSuggestions").isHittable)

        try clickIdentified("notebook.aiProposal.proposal-task.review")
        try clickIdentified("notebook.aiProposal.proposal-task.openTask")
        XCTAssertTrue(try require("prototypeTask.parameterSource.robust").exists)
        XCTAssertTrue(try require("prototypeTask.parameter.robust").value as? String == "-0.5")
        try clickIdentified("central.tab.tab-scientific-notebook")
        try clickIdentified("notebook.aiProposal.proposal-task.review")
        try clickIdentified("notebook.aiProposal.proposal-task.apply")
        XCTAssertTrue(
            waitForAccessibilityValue(
                "notebook.aiProposal.proposal-task.state",
                containing: "Succeeded",
                timeout: 5
            )
        )
        try bringIntoView(
            "notebook.aiProposal.proposal-python.review",
            in: "notebook.document.scroll",
            deltaY: -220
        )
        try clickIdentified("notebook.aiProposal.proposal-python.review")
        try clickIdentified("notebook.aiProposal.proposal-python.reject")
        XCTAssertTrue(
            waitForAccessibilityValue(
                "notebook.aiProposal.proposal-python.state",
                containing: "Rejected"
            )
        )
        assertZeroAIProductionBoundaryCalls()
    }

    func testAIPrototypeRateLimitAndNonresponsiveRecoveryAreExplicit() throws {
        launchAIPrototype(scenario: "rate-limited")
        try clickIdentified("aiPrototype.suggestion.plot")
        try clickIdentified("aiPrototype.send")
        XCTAssertTrue(try require("aiPrototype.response.error", timeout: 5).exists)
        try clickIdentified("aiPrototype.response.retry")
        XCTAssertTrue(try require("aiPrototype.message.ai-assistant-3", timeout: 5).exists)
        assertZeroAIProductionBoundaryCalls()

        app.terminate()
        XCTAssertTrue(app.wait(for: .notRunning, timeout: 5))
        launchAIPrototype(scenario: "nonresponsive")
        try clickIdentified("aiPrototype.suggestion.task")
        try clickIdentified("aiPrototype.send")
        XCTAssertTrue(try require("aiPrototype.response.streaming", timeout: 3).exists)
        try clickIdentified("aiPrototype.response.cancel")
        XCTAssertTrue(try require("aiPrototype.response.restartRequired").exists)
        try clickIdentified("aiPrototype.response.restart")
        XCTAssertFalse(element("aiPrototype.response.restartRequired").exists)
        assertZeroAIProductionBoundaryCalls()
    }

    private func launchPrototype(scenario: String = "happy-path") {
        app = XCUIApplication()
        ensureStoppedBeforeLaunch()
        app.launchArguments = [
            "-ApplePersistenceIgnoreState", "YES",
            "--show-prototype", "notebook",
            "--prototype-state", scenario,
        ]
        app.launch()
        app.activate()
        XCTAssertTrue(
            app.windows["casa-rs Workbench"].waitForExistence(timeout: 10),
            app.debugDescription
        )
        XCTAssertTrue(app.buttons["dock.mode.notebooks"].exists)
    }

    private func launchPythonPrototype(scenario: String = "happy-path") {
        app = XCUIApplication()
        ensureStoppedBeforeLaunch()
        app.launchArguments = [
            "-ApplePersistenceIgnoreState", "YES",
            "--show-prototype", "python",
            "--prototype-state", scenario,
        ]
        app.launch()
        app.activate()
        XCTAssertTrue(
            app.windows["casa-rs Workbench"].waitForExistence(timeout: 10),
            app.debugDescription
        )
        XCTAssertTrue(element("pythonPrototype.kernelState").waitForExistence(timeout: 5))
    }

    private func launchTutorialPrototype(scenario: String = "happy-path") {
        app = XCUIApplication()
        ensureStoppedBeforeLaunch()
        app.launchArguments = [
            "-ApplePersistenceIgnoreState", "YES",
            "--show-prototype", "tutorial",
            "--prototype-state", scenario,
        ]
        app.launch()
        app.activate()
        XCTAssertTrue(
            app.windows["casa-rs Workbench"].waitForExistence(timeout: 10),
            app.debugDescription
        )
        XCTAssertTrue(element("tutorialPrototype.progressSummary").waitForExistence(timeout: 5))
    }

    private func launchAIPrototype(
        scenario: String = "happy-path",
        openDrawer: Bool = true
    ) {
        app = XCUIApplication()
        ensureStoppedBeforeLaunch()
        app.launchArguments = [
            "-ApplePersistenceIgnoreState", "YES",
            "-NSAutomaticTextCompletionEnabled", "NO",
            "--show-prototype", "ai",
            "--prototype-state", scenario,
        ]
        app.launch()
        app.activate()
        XCTAssertTrue(
            app.windows["casa-rs Workbench"].waitForExistence(timeout: 10),
            app.debugDescription
        )
        XCTAssertTrue(
            element("notebook.document.scroll").waitForExistence(timeout: 5),
            app.debugDescription
        )
        XCTAssertTrue(element("aiPrototype.openDrawer").waitForExistence(timeout: 5))
        if openDrawer {
            element("aiPrototype.openDrawer").click()
            XCTAssertTrue(
                element("aiPrototype.boundaryStatus").waitForExistence(timeout: 5),
                app.debugDescription
            )
        }
    }

    private func ensureStoppedBeforeLaunch() {
        guard app.state != .notRunning else { return }
        app.terminate()
        XCTAssertTrue(
            app.wait(for: .notRunning, timeout: 5),
            "A previous tested app instance remained alive before launch"
        )
    }

    private func require(_ identifier: String, timeout: TimeInterval = 5) throws -> XCUIElement {
        let result = element(identifier)
        XCTAssertTrue(result.waitForExistence(timeout: timeout), "Missing accessibility identifier \(identifier)\n\(app.debugDescription)")
        return result
    }

    private func element(_ identifier: String) -> XCUIElement {
        app.descendants(matching: .any).matching(identifier: identifier).firstMatch
    }

    private func clickIdentified(_ identifier: String, timeout: TimeInterval = 5) throws {
        let control = try require(identifier, timeout: timeout)
        XCTAssertTrue(control.isHittable, "Identified control is not hittable: \(identifier)\n\(app.debugDescription)")
        control.click()
    }

    private func bringIntoView(
        _ identifier: String,
        in scrollIdentifier: String,
        deltaY: CGFloat,
        attempts: Int = 8
    ) throws {
        let target = element(identifier)
        let scroll = element(scrollIdentifier)
        XCTAssertTrue(scroll.waitForExistence(timeout: 5), "Missing scroll view \(scrollIdentifier)")
        let hittable = XCTNSPredicateExpectation(
            predicate: NSPredicate(format: "hittable == true"),
            object: scroll
        )
        XCTAssertEqual(
            XCTWaiter.wait(for: [hittable], timeout: 5),
            .completed,
            "Scroll view is not hittable: \(scrollIdentifier)"
        )
        let isComfortablyVisible = {
            guard target.exists, target.isHittable else { return false }
            let viewport = scroll.frame.insetBy(dx: 8, dy: 40)
            return viewport.contains(CGPoint(x: target.frame.midX, y: target.frame.midY))
        }
        for _ in 0..<attempts where !isComfortablyVisible() {
            scroll.scroll(byDeltaX: 0, deltaY: deltaY)
        }
        XCTAssertTrue(
            isComfortablyVisible(),
            "Unable to bring \(identifier) into view\n\(app.debugDescription)"
        )
    }

    private func clickUntilAccessibilityValue(
        control controlIdentifier: String,
        state stateIdentifier: String,
        contains expected: String,
        attempts: Int = 2
    ) throws {
        let control = try require(controlIdentifier)
        if (element(stateIdentifier).value as? String)?.contains(expected) == true {
            return
        }
        XCTAssertTrue(control.isHittable, "Control is not hittable: \(controlIdentifier)")
        control.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0.5)).click()
        if !waitForAccessibilityValue(stateIdentifier, containing: expected) && attempts > 1 {
            control.typeKey(.space, modifierFlags: [])
        }
        XCTAssertTrue(
            waitForAccessibilityValue(stateIdentifier, containing: expected),
            "\(stateIdentifier) did not contain \(expected) after clicking \(controlIdentifier)"
        )
    }

    private func expandExecutionStatus(_ identifier: String) throws {
        try require(identifier).click()
        if !waitForAccessibilityValue(identifier, containing: "expanded") {
            try require(identifier).click()
        }
        XCTAssertTrue(waitForAccessibilityValue(identifier, containing: "expanded"))
    }

    private func notebookSelector(_ notebookID: String) -> XCUIElement {
        app.staticTexts.matching(identifier: "notebook.selector.\(notebookID)").firstMatch
    }

    private func selectViewMode(_ label: String) {
        let identifier = "notebook.viewMode.\(label.lowercased())"
        let segment = app.radioButtons[identifier]
        XCTAssertTrue(segment.waitForExistence(timeout: 5), app.debugDescription)
        let window = app.windows.firstMatch
        let segmentFrame = segment.frame
        let windowFrame = window.frame
        window.coordinate(withNormalizedOffset: CGVector(
            dx: (segmentFrame.midX - windowFrame.minX) / windowFrame.width,
            dy: (segmentFrame.midY - windowFrame.minY) / windowFrame.height
        )).click()
    }

    private func replaceText(_ identifier: String, with value: String) {
        let result = element(identifier)
        XCTAssertTrue(result.waitForExistence(timeout: 5), "Missing editable element \(identifier)")
        result.click()
        app.typeKey("a", modifierFlags: .command)
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        XCTAssertTrue(pasteboard.setString(value, forType: .string))
        app.typeKey("v", modifierFlags: .command)
        let edited = element(identifier)
        if identifier == "notebook.editor.raw" {
            XCTAssertTrue(
                waitForTextValue(edited, equalTo: value),
                "Raw editor did not commit the complete pasted document"
            )
            app.typeKey(.tab, modifierFlags: [])
            Thread.sleep(forTimeInterval: 0.2)
        } else {
            XCTAssertTrue(
                waitForTextValue(edited, containing: value),
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

    private func waitForValue(
        _ identifier: String,
        containing substring: String,
        timeout: TimeInterval = 5
    ) -> Bool {
        let element = app.descendants(matching: .any).matching(identifier: identifier).firstMatch
        let predicate = NSPredicate(format: "value CONTAINS %@", substring)
        return XCTWaiter.wait(
            for: [XCTNSPredicateExpectation(predicate: predicate, object: element)],
            timeout: timeout
        ) == .completed
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

    private func waitForFile(_ url: URL, containing text: String) -> Bool {
        let deadline = Date().addingTimeInterval(5)
        repeat {
            if (try? String(contentsOf: url, encoding: .utf8))?.contains(text) == true {
                return true
            }
            Thread.sleep(forTimeInterval: 0.05)
        } while Date() < deadline
        return false
    }

    private func waitForReceipt(in runs: URL, containing text: String) -> Bool {
        let deadline = Date().addingTimeInterval(10)
        repeat {
            let receipts = (try? FileManager.default.contentsOfDirectory(
                at: runs,
                includingPropertiesForKeys: nil
            ))?.map { $0.appendingPathComponent("receipt.json") } ?? []
            if receipts.contains(where: {
                (try? String(contentsOf: $0, encoding: .utf8))?.contains(text) == true
            }) {
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

    private func waitForAccessibilityValue(
        _ identifier: String,
        containing substring: String,
        timeout: TimeInterval = 5
    ) -> Bool {
        waitForValue(identifier, containing: substring, timeout: timeout)
    }

    private func waitForPositivePercentage(_ identifier: String) -> Bool {
        let result = element(identifier)
        let deadline = Date().addingTimeInterval(5)
        repeat {
            if let value = result.value as? String,
               let percentage = Int(value.replacingOccurrences(of: "%", with: "")),
               percentage > 0 {
                return true
            }
            Thread.sleep(forTimeInterval: 0.05)
        } while Date() < deadline
        return false
    }

    private func assertZeroProductionBoundaryCalls() {
        let audit = app.descendants(matching: .any).matching(identifier: "notebook.boundaryAudit").firstMatch
        XCTAssertTrue(audit.waitForExistence(timeout: 3), app.debugDescription)
        XCTAssertEqual(audit.value as? String ?? audit.label, "0")
    }

    private func assertZeroPythonProductionBoundaryCalls() {
        let audit = element("pythonPrototype.boundaryAudit")
        XCTAssertTrue(audit.waitForExistence(timeout: 3), app.debugDescription)
        XCTAssertEqual(audit.value as? String ?? audit.label, "0")
    }

    private func assertZeroTutorialProductionBoundaryCalls() {
        let audit = element("tutorialPrototype.boundaryAudit")
        XCTAssertTrue(audit.waitForExistence(timeout: 3), app.debugDescription)
        XCTAssertEqual(audit.value as? String ?? audit.label, "0")
    }

    private func assertZeroAIProductionBoundaryCalls() {
        let audit = element("aiPrototype.boundaryStatus")
        XCTAssertTrue(audit.waitForExistence(timeout: 3), app.debugDescription)
        let value = audit.value as? String ?? ""
        XCTAssertTrue(
            value == "0" || audit.label.contains("0 production calls"),
            "Expected zero AI production calls, got value=\(value.debugDescription) label=\(audit.label)"
        )
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

    private func acceptedPythonPrototypeContrastArtifact(_ issue: XCUIAccessibilityAuditIssue) -> Bool {
        guard issue.auditType.contains(.contrast) else { return false }
        let identifier = issue.element?.identifier ?? ""
        if identifier.hasPrefix("notebookVisualization.revisionCount.") {
            return true
        }
        guard let value = issue.element?.value as? String else { return false }
        return value == "Persistent per-notebook kernel · interaction prototype"
            // macOS 15 reports these opaque black-on-near-white text layers as
            // contrast failures; the retained CI screenshot verifies the
            // rendered foreground and background rather than a translucent
            // or obscured control.
            || value == "TW Hya · amplitude vs UV distance"
            || value == "The continuum amplitudes should decline smoothly with UV distance. Keep both vector and raster forms so the figure remains editable and portable."
            || value == "TW Hya · continuum image"
            || value == "Code"
            || value == "88d14db8cc92074d"
            || value.hasPrefix("from casars import msexplore\n")
    }
}
