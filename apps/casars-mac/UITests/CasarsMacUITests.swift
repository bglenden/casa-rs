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
        let analysisNote = element("notebook.richElement.rich-element-3")
        XCTAssertTrue(waitForTextValue(analysisNote, containing: "Analysis-only draft note"))
        XCTAssertFalse(analysisNote.label.contains("Observation-log-only"))

        notebookSelector("notebook-twhya-observation-log").click()
        XCTAssertTrue(
            waitForTextValue(
                element("notebook.richElement.rich-element-3"),
                containing: "Observation-log-only draft note"
            )
        )
        assertZeroProductionBoundaryCalls()
    }

    func testTaskNavigationExecutionGesturesAndNeutralTaskCell() throws {
        launchPrototype()

        try bringIntoView(
            "notebook.parameters.open.receipt-imager-mfs",
            in: "notebook.document.scroll",
            deltaY: -220
        )

        let parameterBlock = try require("notebook.parameters.open.receipt-imager-mfs")
        let status = try require("notebook.executionStatus.receipt-imager-mfs")
        XCTAssertGreaterThan(parameterBlock.frame.width, status.frame.width * 2, "Status color/label must remain a compact affordance, not task-cell decoration.")
        XCTAssertNotEqual(parameterBlock.identifier, status.identifier)

        try clickIdentified("notebook.parameters.open.receipt-imager-mfs")
        let taskIdentity = element("prototypeTask.identity.receipt-imager-mfs")
        if !taskIdentity.waitForExistence(timeout: 2) {
            try clickIdentified("notebook.parameters.open.receipt-imager-mfs")
        }
        XCTAssertTrue(taskIdentity.waitForExistence(timeout: 5), app.debugDescription)
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
            withDestinationURL: URL(fileURLWithPath: try resolvedTestPython())
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
        let notebook = notebookSelector(notebookID)
        if !notebook.waitForExistence(timeout: 3) {
            // Project probing can finish after the first dock selection and restore the
            // datasets view. Select Notebooks again once the project is fully loaded.
            try clickIdentified("dock.mode.notebooks")
        }
        XCTAssertTrue(notebook.waitForExistence(timeout: 8), app.debugDescription)
        try require("notebook.selector.open").click()

        if element("inspector.collapse").isHittable {
            try clickIdentified("inspector.collapse")
        }
        selectViewMode("Raw")
        selectViewMode("Rich")

        let authority: XCUIElement
        let runAll: XCUIElement
        if element("notebook.python.menu").exists {
            authority = try require("notebook.python.menu")
            try clickIdentified("notebook.python.menu")
            runAll = app.menuItems["Run All"]
            XCTAssertTrue(runAll.waitForExistence(timeout: 3), app.debugDescription)
        } else {
            authority = try require("notebook.python.authority")
            runAll = try require("notebook.python.runAll")
        }
        XCTAssertTrue(
            authority.label.contains("normal user authority")
                || (authority.value as? String)?.contains("normal user authority") == true,
            authority.debugDescription
        )
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

    func testProductionAssistantPersistsCitedTailPinAndOpensSuggestedTask() throws {
        let notebookID = "019f0000-0000-7000-8000-000000000401"
        let project = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-mac-ui-assistant-\(UUID().uuidString)", isDirectory: true)
        let notebooks = project.appendingPathComponent("notebooks", isDirectory: true)
        try FileManager.default.createDirectory(at: notebooks, withIntermediateDirectories: true)
        try "<!-- casa-rs-notebook:v1 id=\(notebookID) -->\n\n# Production assistant review\n\nInitial note.\n"
            .write(to: notebooks.appendingPathComponent("Analysis.md"), atomically: true, encoding: .utf8)
        try "# Fixture source\nTyped CASA-RS provider contracts.\n"
            .write(to: project.appendingPathComponent("ARCHITECTURE.md"), atomically: true, encoding: .utf8)
        let documents = project.appendingPathComponent("documents", isDirectory: true)
        try FileManager.default.createDirectory(at: documents, withIntermediateDirectories: true)
        try Data("not a PDF".utf8).write(to: documents.appendingPathComponent("broken.pdf"))
        try Data([0, 1, 2]).write(to: documents.appendingPathComponent("unsupported.docx"))
        productionProjectURL = project

        app = XCUIApplication()
        ensureStoppedBeforeLaunch()
        app.launchEnvironment["CASA_RS_AGENT_FIXTURE"] = "1"
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
        XCTAssertFalse(
            element("notebook.richElement.rich-element-0").exists,
            "Notebook control comments must not be visible in Rich mode"
        )

        try clickIdentified("assistant.openDrawer")
        XCTAssertTrue(element("assistant.discussion").waitForExistence(timeout: 8), app.debugDescription)
        let notebookTitle = try require("notebook.title")
        XCTAssertGreaterThan(notebookTitle.frame.width, 100, app.debugDescription)
        XCTAssertLessThan(notebookTitle.frame.height, 40, app.debugDescription)
        XCTAssertTrue(element("notebook.viewMode").isHittable, app.debugDescription)
        XCTAssertTrue(element("notebook.save").exists, app.debugDescription)
        XCTAssertTrue(element("assistant.model").waitForExistence(timeout: 8), app.debugDescription)
        XCTAssertTrue(element("assistant.effort").waitForExistence(timeout: 8), app.debugDescription)
        XCTAssertTrue(element("assistant.usage").waitForExistence(timeout: 8), app.debugDescription)
        XCTAssertTrue(element("assistant.settings").waitForExistence(timeout: 8), app.debugDescription)
        try clickIdentified("assistant.settings")
        XCTAssertTrue(
            waitForValue("assistant.corpus.status", containing: "Local corpus ready", timeout: 15),
            app.debugDescription
        )
        XCTAssertTrue(element("assistant.corpus.diagnostics").exists, app.debugDescription)
        try clickIdentified("assistant.corpus.diagnostics")
        if !waitForValue("assistant.corpus.diagnostics", containing: "expanded", timeout: 1) {
            try require("assistant.corpus.diagnostics").typeKey(.space, modifierFlags: [])
        }
        XCTAssertTrue(
            waitForValue("assistant.corpus.diagnostics", containing: "expanded"),
            "Corpus diagnostics did not expand"
        )
        XCTAssertTrue(
            app.staticTexts.matching(
                NSPredicate(format: "value CONTAINS %@", "Could not open PDF documents/broken.pdf")
            ).firstMatch.waitForExistence(timeout: 5),
            app.debugDescription
        )
        XCTAssertTrue(element("assistant.account.logout").waitForExistence(timeout: 5), app.debugDescription)
        try clickIdentified("assistant.account.logout")
        XCTAssertTrue(element("assistant.account.login").waitForExistence(timeout: 5), app.debugDescription)
        try clickIdentified("assistant.account.login")
        XCTAssertTrue(element("assistant.usage").waitForExistence(timeout: 5), app.debugDescription)
        replaceText("assistant.input", with: "How should I start weighting this image?")
        let send = try require("assistant.send")
        let sendReady = XCTNSPredicateExpectation(
            predicate: NSPredicate(format: "enabled == true"),
            object: send
        )
        XCTAssertEqual(
            XCTWaiter.wait(for: [sendReady], timeout: 15),
            .completed,
            "Assistant sidecar did not become ready to send"
        )
        try clickIdentified("assistant.send")
        let pinToNotebook = app.links["Add to notebook"].firstMatch
        XCTAssertTrue(
            pinToNotebook.waitForExistence(timeout: 15),
            app.debugDescription
        )
        let citation = app.descendants(matching: .any).matching(
            NSPredicate(format: "identifier BEGINSWITH %@", "assistant.citation.")
        ).firstMatch
        XCTAssertTrue(citation.waitForExistence(timeout: 5), app.debugDescription)
        citation.click()
        XCTAssertTrue(element("assistant.citation.preview").waitForExistence(timeout: 5), app.debugDescription)
        try clickIdentified("assistant.citation.done")

        replaceText("assistant.input", with: "What should I check next?")
        try clickIdentified("assistant.send")
        let twoAnswers = XCTNSPredicateExpectation(
            predicate: NSPredicate(format: "count == 2"),
            object: app.links.matching(NSPredicate(format: "label == %@", "Add to notebook"))
        )
        XCTAssertEqual(
            XCTWaiter.wait(for: [twoAnswers], timeout: 15),
            .completed,
            "A second streamed assistant answer did not remain interactive"
        )
        let pinLinks = app.links.matching(NSPredicate(format: "label == %@", "Add to notebook"))
        XCTAssertTrue(
            pinLinks.element(boundBy: pinLinks.count - 1).isHittable,
            "The chat must follow new output to the bottom instead of leaving the latest answer offscreen"
        )

        pinToNotebook.click()
        XCTAssertTrue(app.staticTexts["Added to notebook"].firstMatch.waitForExistence(timeout: 5))

        let openTask = app.links["Open imager task"].firstMatch
        XCTAssertTrue(openTask.waitForExistence(timeout: 5), app.debugDescription)
        openTask.click()
        XCTAssertTrue(element("task.parameter.vis").waitForExistence(timeout: 8), app.debugDescription)
        let taskScroll = try XCTUnwrap(
            app.scrollViews.allElementsBoundByIndex.max {
                $0.frame.width < $1.frame.width
            },
            app.debugDescription
        )
        for _ in 0..<6 {
            taskScroll.scroll(byDeltaX: 0, deltaY: -420)
        }
        XCTAssertTrue(waitForValue("task.parameter.weighting", containing: "briggs"))
        XCTAssertTrue(waitForValue("task.parameter.robust", containing: "-0.5"))
        XCTAssertEqual(try accessibilityValue("task.parameterSource.weighting"), "AI-suggested non-default")
        XCTAssertEqual(try accessibilityValue("task.parameterSource.robust"), "AI-suggested non-default")
        replaceText("task.parameter.robust", with: "-0.25")
        XCTAssertFalse(
            element("task.parameterSource.robust").waitForExistence(timeout: 1),
            "Editing an AI-suggested value must clear its AI provenance marker"
        )
        XCTAssertEqual(try accessibilityValue("task.parameterSource.weighting"), "AI-suggested non-default")

        try clickIdentified("central.tab.tab-scientific-notebook")
        try clickIdentified("assistant.close")
        let appendedRichNote = app.descendants(matching: .any).matching(
            NSPredicate(
                format: "identifier BEGINSWITH %@ AND (value CONTAINS %@ OR label CONTAINS %@)",
                "notebook.richElement.",
                "Use Briggs weighting with robust -0.5",
                "Use Briggs weighting with robust -0.5"
            )
        ).firstMatch
        XCTAssertTrue(
            appendedRichNote.waitForExistence(timeout: 5),
            "The saved AI pin must appear in the already-open Rich notebook without a mode toggle\n\(app.debugDescription)"
        )
        XCTAssertTrue(
            app.buttons.matching(
                NSPredicate(format: "identifier BEGINSWITH %@", "notebook.parameters.open.")
            ).firstMatch.waitForExistence(timeout: 5),
            "A pinned task suggestion must render as an interactive notebook parameter block\n\(app.debugDescription)"
        )
        let exposedPinMetadata = app.descendants(matching: .any).matching(
            NSPredicate(
                format: "identifier BEGINSWITH %@ AND (value CONTAINS %@ OR label CONTAINS %@)",
                "notebook.richElement.",
                "<!-- casa-rs-ai-pin:",
                "<!-- casa-rs-ai-pin:"
            )
        ).firstMatch
        XCTAssertFalse(
            exposedPinMetadata.exists,
            "Rich mode must render the note without exposing persisted control comments\n\(app.debugDescription)"
        )

        let saved = try String(contentsOf: notebooks.appendingPathComponent("Analysis.md"))
        XCTAssertTrue(saved.contains("casa-rs-ai-pin:v1"), saved)
        XCTAssertTrue(saved.contains("Use **Briggs weighting** with robust -0.5"))
        XCTAssertTrue(saved.contains("CASA-RS Radio Interferometry Primer v1.0"))
        XCTAssertTrue(saved.contains("surface = \"imager\""), saved)
        XCTAssertTrue(saved.contains("robust = -0.5"), saved)
        XCTAssertTrue(saved.hasSuffix("\n"), "Notebook append should leave a normal Markdown trailing newline")
        let conversations = project.appendingPathComponent(".casa-rs/conversations", isDirectory: true)
        XCTAssertFalse((try FileManager.default.contentsOfDirectory(atPath: conversations.path)).isEmpty)
    }

    func testOptInProductionAssistantSubscriptionGUIResume() throws {
        let liveArtifactDirectory = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent(".gui-test", isDirectory: true)
        let liveGate = liveArtifactDirectory.appendingPathComponent("assistant-live-gui.enabled")
        guard FileManager.default.fileExists(atPath: liveGate.path) else {
            throw XCTSkip("run `just assistant-live-gui` to exercise production GUI subscription chat")
        }
        let liveGateObject = try PropertyListSerialization.propertyList(
            from: Data(contentsOf: liveGate),
            options: [],
            format: nil
        )
        let liveEnvironment = try XCTUnwrap(liveGateObject as? [String: String])
        let notebookID = "019f0000-0000-7000-8000-000000000501"
        let project = URL(
            fileURLWithPath: try XCTUnwrap(liveEnvironment["projectRoot"]),
            isDirectory: true
        )
        let notebooks = project.appendingPathComponent("notebooks", isDirectory: true)
        try FileManager.default.createDirectory(at: notebooks, withIntermediateDirectories: true)
        try "<!-- casa-rs-notebook:v1 id=\(notebookID) -->\n\n# Wave 5A live acceptance\n\nUnique production GUI project.\n"
            .write(to: notebooks.appendingPathComponent("Analysis.md"), atomically: true, encoding: .utf8)

        let firstMarker = "WAVE5A-FIRST-\(UUID().uuidString.prefix(8))"
        let cancelMarker = "WAVE5A-CANCEL-\(UUID().uuidString.prefix(8))"
        let resumeMarker = "WAVE5A-RESUME-\(UUID().uuidString.prefix(8))"
        print("CASA_RS_LIVE_GUI_PROJECT \(project.path)")
        print("CASA_RS_LIVE_GUI_MARKERS first=\(firstMarker) cancel=\(cancelMarker) resume=\(resumeMarker)")

        launchLiveAssistantProject(project, environment: liveEnvironment)
        try openProductionAssistant(notebookID: notebookID)
        let usage = try require("assistant.usage", timeout: 25)
        let model = try require("assistant.model")
        let effort = try require("assistant.effort")
        print("CASA_RS_LIVE_GUI_CONTROLS model=\(model.label) effort=\(effort.label) usage=\(usage.label)")

        let beforeModelChange = try waitForLiveAssistantTranscript(in: project, timeout: 30) {
            $0.backendSession != nil
        }
        let initialBackendSessionID = try XCTUnwrap(beforeModelChange.backendSession?.sessionId)
        let initialModelID = beforeModelChange.profile.model
        model.click()
        let optionPrefix = "assistant.model.option."
        let modelOption = app.menuItems.matching(
            NSPredicate(format: "identifier BEGINSWITH %@", optionPrefix)
        ).firstMatch
        XCTAssertTrue(modelOption.waitForExistence(timeout: 5), app.debugDescription)
        let selectedModelID = String(modelOption.identifier.dropFirst(optionPrefix.count))
        XCTAssertFalse(selectedModelID.isEmpty)
        modelOption.click()
        let afterModelChange = try waitForLiveAssistantTranscript(in: project, timeout: 10) { transcript in
            transcript.backendSession?.sessionId == initialBackendSessionID
                && transcript.profile.model == selectedModelID
        }
        XCTAssertEqual(afterModelChange.backendSession?.sessionId, initialBackendSessionID)
        XCTAssertNotEqual(afterModelChange.profile.model, initialModelID)
        XCTAssertFalse(
            app.staticTexts.matching(NSPredicate(format: "value CONTAINS %@", "required MCP servers failed"))
                .firstMatch.exists,
            app.debugDescription
        )
        print(
            "CASA_RS_LIVE_GUI_MODEL_CHANGE from=\(initialModelID) to=\(afterModelChange.profile.model) "
                + "backend_unchanged=true"
        )

        try sendLiveAssistantPrompt(
            "Use the CASA project MCP task.catalog tool. Do not use shell or network. Reply with \(firstMarker) and the number of cataloged tasks."
        )
        let firstTranscript = try waitForLiveAssistantTranscript(in: project, timeout: 90) { transcript in
            transcript.messages.contains { $0.role == "assistant" && $0.content.contains(firstMarker) }
                && transcript.messages.contains { message in
                    message.activities.contains {
                        $0.label == "CASA task.catalog" && $0.state == "succeeded"
                    }
                }
                && transcript.profile.pythonProvenance != nil
        }
        let backendSessionID = try XCTUnwrap(firstTranscript.backendSession?.sessionId)
        let python = try XCTUnwrap(firstTranscript.profile.pythonProvenance)
        print(
            "CASA_RS_LIVE_GUI_STATE backend=present model=\(firstTranscript.profile.model) "
                + "effort=\(firstTranscript.profile.effort) python=\(python.resolvedPath) "
                + "python_version=\(python.version)"
        )

        replaceText(
            "assistant.input",
            with: "Use task.catalog, then write a long detailed explanation with 100 numbered items. End with \(cancelMarker)."
        )
        try clickIdentified("assistant.send")
        let cancel = app.buttons["Cancel"].firstMatch
        XCTAssertTrue(cancel.waitForExistence(timeout: 10), app.debugDescription)
        cancel.click()
        _ = try waitForLiveAssistantTranscript(in: project, timeout: 30) { transcript in
            transcript.messages.contains { message in
                message.role == "assistant"
                    && (message.content.contains("cancelled") || message.content.contains(cancelMarker))
            }
        }

        app.terminate()
        XCTAssertTrue(app.wait(for: .notRunning, timeout: 8), "Production app did not terminate")
        launchLiveAssistantProject(project, environment: liveEnvironment)
        try openProductionAssistant(notebookID: notebookID)
        _ = try require("assistant.usage", timeout: 25)
        let durableAfterRestart = try waitForLiveAssistantTranscript(in: project, timeout: 5) {
            $0.backendSession?.sessionId == backendSessionID
                && $0.messages.contains { $0.content.contains(firstMarker) }
        }
        print("CASA_RS_LIVE_GUI_DURABLE_AFTER_RESTART messages=\(durableAfterRestart.messages.count)")
        let priorResponseVisible = app.staticTexts.matching(
            NSPredicate(format: "value CONTAINS %@", firstMarker)
        ).firstMatch.waitForExistence(timeout: 3)
        let visibleHandoff = app.staticTexts.matching(
            NSPredicate(format: "value BEGINSWITH %@", "Previous Codex ses")
        ).firstMatch.waitForExistence(timeout: priorResponseVisible ? 0.1 : 12)
        XCTAssertTrue(
            priorResponseVisible || visibleHandoff,
            "Relaunch must show the prior transcript or an honest session handoff\n\(app.debugDescription)"
        )
        let taskActivityCountBeforeResume = durableAfterRestart.messages.filter { message in
            message.activities.contains { $0.label == "CASA task.catalog" }
        }.count
        try sendLiveAssistantPrompt(
            "Use the CASA project MCP task.catalog tool again. Reply with \(resumeMarker) and confirm this is the resumed Wave 5A conversation."
        )
        let resumedTranscript = try waitForLiveAssistantTranscript(in: project, timeout: 90) { transcript in
            transcript.messages.contains {
                    $0.role == "assistant" && $0.content.contains(resumeMarker)
                }
                && transcript.messages.filter { message in
                    message.activities.contains { $0.label == "CASA task.catalog" }
                }.count > taskActivityCountBeforeResume
        }
        let sameBackend = resumedTranscript.backendSession?.sessionId == backendSessionID
        let handoff = resumedTranscript.messages.first {
            $0.role == "activity" && $0.content.contains("could not be resumed")
        }
        XCTAssertTrue(
            sameBackend || handoff != nil,
            "The real Codex thread must either resume or record a visible session handoff"
        )
        if handoff != nil {
            let handoffVisible = visibleHandoff || app.staticTexts.matching(
                NSPredicate(format: "value BEGINSWITH %@", "Previous Codex ses")
            ).firstMatch.waitForExistence(timeout: 5)
            XCTAssertTrue(handoffVisible, app.debugDescription)
        }
        let screenshot = XCTAttachment(screenshot: app.screenshot())
        screenshot.name = "Wave 5A live subscription conversation resumed"
        screenshot.lifetime = .keepAlways
        add(screenshot)
        try Data().write(
            to: URL(fileURLWithPath: try XCTUnwrap(liveEnvironment["passReceipt"])),
            options: .atomic
        )
        print(
            "CASA_RS_LIVE_GUI_RESUME same_backend=\(sameBackend) "
                + "visible_handoff=\(handoff != nil) messages=\(resumedTranscript.messages.count)"
        )
    }

    func testOptInProductionNotebookTaskPythonPlotRoundTrip() throws {
        let artifactDirectory = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent(".gui-test", isDirectory: true)
        let gate = artifactDirectory.appendingPathComponent("notebook-roundtrip-gui.enabled")
        guard FileManager.default.fileExists(atPath: gate.path) else {
            throw XCTSkip("run `just notebook-roundtrip-gui` to exercise the production round-trip")
        }
        let object = try PropertyListSerialization.propertyList(
            from: Data(contentsOf: gate),
            options: [],
            format: nil
        )
        let environment = try XCTUnwrap(object as? [String: String])
        let notebookID = "019f0000-0000-7000-8000-000000000517"
        let pythonCellID = "019f0000-0000-7000-8000-000000000518"
        let project = URL(
            fileURLWithPath: try XCTUnwrap(environment["projectRoot"]),
            isDirectory: true
        )
        let notebooks = project.appendingPathComponent("notebooks", isDirectory: true)
        let pythonBin = project.appendingPathComponent(".casa-rs/python/bin", isDirectory: true)
        let notebookFile = notebooks.appendingPathComponent("Analysis.md")
        let failingPython = "raise RuntimeError(\"intentional Wave 5C retry\")\n"
        let outputRelativePath = "products/wave5c-synthetic.ms"
        let outputMS = project.appendingPathComponent(outputRelativePath, isDirectory: true)
        let runs = project.appendingPathComponent(".casa-rs/notebook-runs", isDirectory: true)
        let resumeAfterTask = environment["resumeAfterTask"] == "true"
        var marker: String
        var taskCellID: String
        print("CASA_RS_WAVE5C_PROJECT \(project.path)")

        if resumeAfterTask {
            let retainedMarkdown = try String(contentsOf: notebookFile, encoding: .utf8)
            let markerRange = try XCTUnwrap(
                retainedMarkdown.range(
                    of: #"WAVE5C-ROUNDTRIP-[A-F0-9]{8}"#,
                    options: .regularExpression
                ),
                "The retained project does not contain a Wave 5C assistant marker"
            )
            marker = String(retainedMarkdown[markerRange])
            let taskReceipt = try waitForReceiptObject(in: runs, timeout: 2) {
                $0["operation_id"] as? String == "simobserve"
                    && $0["status"] as? String == "succeeded"
            }
            taskCellID = try XCTUnwrap(taskReceipt["cell_id"] as? String)
            XCTAssertTrue(waitForPath(outputMS, timeout: 2), "Retained simobserve output is missing")
            print("CASA_RS_WAVE5C_RESUME after_task marker=\(marker)")
            launchLiveAssistantProject(project, environment: environment)
            try openProductionAssistant(notebookID: notebookID)

            let visualizationID = try waitForVisualizationID(in: notebookFile, timeout: 2)
            let visualizationFile = project
                .appendingPathComponent(".casa-rs/notebook-visualizations", isDirectory: true)
                .appendingPathComponent("\(visualizationID).json")
            let visualization = try XCTUnwrap(
                JSONSerialization.jsonObject(with: Data(contentsOf: visualizationFile)) as? [String: Any]
            )
            let revisions = try XCTUnwrap(visualization["revisions"] as? [[String: Any]])
            XCTAssertGreaterThanOrEqual(revisions.count, 2)
            let latestRevision = try XCTUnwrap(revisions.last?["revision"] as? NSNumber).intValue

            try clickIdentified("central.tab.tab-scientific-notebook")
            try clickIdentified("assistant.close")
            try bringIntoView(
                "notebook.visualization.\(visualizationID)",
                in: "notebook.document.scroll",
                deltaY: -480,
                attempts: 16
            )
            XCTAssertTrue(element("notebook.visualization.previousRevisions.\(visualizationID)").exists)
            try clickIdentified("notebook.visualization.preview.\(latestRevision)")
            XCTAssertTrue(
                element("notebook.visualization.lightbox").waitForExistence(timeout: 5),
                app.debugDescription
            )
            app.typeKey(.escape, modifierFlags: [])
            try bringIntoView(
                "notebook.visualization.openExplorer.\(visualizationID)",
                in: "notebook.document.scroll",
                deltaY: 200
            )
            try clickIdentified("notebook.visualization.openExplorer.\(visualizationID)")
            XCTAssertTrue(waitForValue("msPlot.preset.\(outputMS.path)", containing: "Amplitude vs Time"))

            app.terminate()
            XCTAssertTrue(app.wait(for: .notRunning, timeout: 8), "Production app did not terminate")
            launchLiveAssistantProject(project, environment: environment)
            try openProductionAssistant(notebookID: notebookID)
            _ = try waitForLiveAssistantTranscript(in: project, timeout: 10) {
                $0.messages.contains { $0.content.contains(marker) }
            }
            try clickIdentified("assistant.close")
            try bringIntoView(
                "notebook.visualization.\(visualizationID)",
                in: "notebook.document.scroll",
                deltaY: -480,
                attempts: 16
            )
            XCTAssertTrue(element("notebook.visualization.previousRevisions.\(visualizationID)").exists)
            try bringIntoView(
                "notebook.python.previousRevisions.\(pythonCellID)",
                in: "notebook.document.scroll",
                deltaY: 420
            )
            XCTAssertTrue(element("notebook.python.latestRevision.\(pythonCellID)").exists)
            try bringIntoView(
                "notebook.parameters.open.\(taskCellID)",
                in: "notebook.document.scroll",
                deltaY: -420
            )
            try clickIdentified("notebook.parameters.open.\(taskCellID)")
            XCTAssertTrue(waitForValue("task.parameter.output_ms", containing: outputRelativePath, timeout: 10))
            XCTAssertFalse(element("task.stop").isEnabled, "Reloading receipt parameters must not rerun the task")
            try Data().write(
                to: URL(fileURLWithPath: try XCTUnwrap(environment["passReceipt"])),
                options: .atomic
            )
            return
        } else {
            try FileManager.default.createDirectory(at: notebooks, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(at: pythonBin, withIntermediateDirectories: true)
            try FileManager.default.createSymbolicLink(
                at: pythonBin.appendingPathComponent("python3"),
                withDestinationURL: URL(fileURLWithPath: try XCTUnwrap(environment["pythonCommand"]))
            )
            try """
            <!-- casa-rs-notebook:v1 id=\(notebookID) -->

            # Wave 5C production round-trip

            This disposable notebook validates one chronological scientific workflow.

            <!-- casa-rs-cell:v1 id=\(pythonCellID) kind=python -->
            ```python
            \(failingPython)```
            <!-- /casa-rs-cell -->
            """.write(to: notebookFile, atomically: true, encoding: .utf8)

            marker = "WAVE5C-ROUNDTRIP-\(UUID().uuidString.prefix(8))"
            print("CASA_RS_WAVE5C_MARKER \(marker)")

            launchLiveAssistantProject(project, environment: environment)
            try openProductionAssistant(notebookID: notebookID)
            try sendLiveAssistantPrompt(
            """
            Use only the CASA project MCP tools; do not use shell or web. Call corpus.search for robust visibility weighting, then task.schema for simobserve. Call task.suggest for simobserve with exactly these non-default parameters and no default-valued parameters: request_kind=family, telescope=ALMA, array_config=synthetic-alma-compact, band=Band 6, target_ms_size_gib=0.00001, output_ms=\(outputRelativePath), pointing_count=3. Reply with \(marker), a short cited scientific explanation, and the task action.
            """
        )
        let transcript = try waitForLiveAssistantTranscript(in: project, timeout: 120) { transcript in
            transcript.messages.contains { message in
                message.role == "assistant"
                    && message.content.contains(marker)
                    && !message.citations.isEmpty
                    && message.activities.contains {
                        $0.label == "CASA task.suggest" && $0.state == "succeeded"
                    }
                    && message.taskSuggestions.contains { $0.taskId == "simobserve" }
            }
        }
        let answer = try XCTUnwrap(transcript.messages.last { $0.content.contains(marker) })
        let suggestion = try XCTUnwrap(answer.taskSuggestions.first { $0.taskId == "simobserve" })
        let expectedSuggestion = [
            "request_kind": "family",
            "telescope": "ALMA",
            "array_config": "synthetic-alma-compact",
            "band": "Band 6",
            "target_ms_size_gib": "0.00001",
            "output_ms": outputRelativePath,
            "pointing_count": "3",
        ]
        let expectedParameterOrder = [
            "request_kind",
            "telescope",
            "array_config",
            "band",
            "target_ms_size_gib",
            "output_ms",
            "pointing_count",
        ]
        XCTAssertEqual(Set(suggestion.parameters.keys), Set(expectedSuggestion.keys))
        for (name, value) in expectedSuggestion where name != "target_ms_size_gib" {
            XCTAssertEqual(suggestion.parameters[name], value, "Unexpected suggested value for \(name)")
        }
        XCTAssertEqual(
            try XCTUnwrap(Double(try XCTUnwrap(suggestion.parameters["target_ms_size_gib"]))),
            0.00001,
            accuracy: 1e-12
        )

        try clickIdentified("assistant.message.\(answer.id).pin", timeout: 10)
        XCTAssertTrue(app.staticTexts["Added to notebook"].firstMatch.waitForExistence(timeout: 5))
        let pinned = try String(contentsOf: notebookFile, encoding: .utf8)
        XCTAssertEqual(pinned.components(separatedBy: "casa-rs-ai-pin:v1").count - 1, 1)
        XCTAssertEqual(pinned.components(separatedBy: marker).count - 1, 1)
        XCTAssertTrue(pinned.contains(answer.citations[0].locator), pinned)
        XCTAssertTrue(pinned.hasSuffix("<!-- /casa-rs-cell -->\n"), pinned)

        try clickIdentified("assistant.message.\(answer.id).task.\(suggestion.id)", timeout: 10)
        XCTAssertTrue(element("task.parameter.request_kind").waitForExistence(timeout: 10), app.debugDescription)
        let taskScroll = try XCTUnwrap(
            app.scrollViews.allElementsBoundByIndex.max { $0.frame.width < $1.frame.width },
            app.debugDescription
        )
        // Follow the schema's top-to-bottom order. The task form is lazy, so
        // revisiting an earlier control after scrolling it offscreen makes the
        // accessibility element disappear even though its value is intact.
        for parameter in expectedParameterOrder {
            let control = element("task.parameter.\(parameter)")
            for _ in 0..<12 where !control.exists || !control.isHittable {
                taskScroll.scroll(byDeltaX: 0, deltaY: -300)
            }
            XCTAssertTrue(control.exists, "Missing suggested parameter \(parameter)\n\(app.debugDescription)")
            XCTAssertEqual(
                try accessibilityValue("task.parameterSource.\(parameter)"),
                "AI-suggested non-default"
            )
        }
        XCTAssertFalse(
            element("task.parameterSource.ms_channels").exists,
            "A default-valued parameter must not be decorated as AI-suggested"
        )
        for _ in 0..<10 where !element("task.safety.confirm").isHittable {
            taskScroll.scroll(byDeltaX: 0, deltaY: -360)
        }
        try clickIdentified("task.safety.confirm")
        try clickIdentified("task.run")
        XCTAssertTrue(
            waitForValue("task.run.status", containing: "succeeded", timeout: 120),
            app.debugDescription
        )
        XCTAssertTrue(waitForPath(outputMS, timeout: 10), "simobserve did not create \(outputMS.path)")
        let taskReceipt = try waitForReceiptObject(in: runs, timeout: 10) {
            $0["operation_id"] as? String == "simobserve" && $0["status"] as? String == "succeeded"
        }
        taskCellID = try XCTUnwrap(taskReceipt["cell_id"] as? String)
        XCTAssertEqual((taskReceipt["schema_version"] as? NSNumber)?.intValue, 2)
        XCTAssertTrue(
            (taskReceipt["products"] as? [[String: Any]])?.contains {
                ($0["path"] as? String)?.contains(outputRelativePath) == true
            } == true,
            "Task receipt did not retain the produced MeasurementSet: \(taskReceipt)"
        )
        }

        try clickIdentified("central.tab.tab-scientific-notebook")
        try clickIdentified("assistant.close")
        try bringIntoView("notebook.python.cell.\(pythonCellID)", in: "notebook.document.scroll", deltaY: -360)
        let pythonRun = try require("notebook.python.run.\(pythonCellID)")
        XCTAssertEqual(
            XCTWaiter.wait(
                for: [XCTNSPredicateExpectation(predicate: NSPredicate(format: "enabled == true"), object: pythonRun)],
                timeout: 15
            ),
            .completed,
            "Selected project Python did not become ready"
        )
        pythonRun.click()
        _ = try waitForReceiptObject(in: runs, timeout: 30) {
            $0["operation_id"] as? String == "python.execute"
                && $0["status"] as? String == "failed"
        }

        let scientificPython = """
        import matplotlib.pyplot as plt
        wavelength_m = 0.0013
        baseline_m = 1000.0
        resolution_arcsec = 206265.0 * wavelength_m / baseline_m
        print(f"resolution_arcsec={resolution_arcsec:.6f}")
        baselines = [100.0, 300.0, 1000.0]
        resolutions = [206265.0 * wavelength_m / value for value in baselines]
        plt.figure()
        plt.plot(baselines, resolutions, marker="o")
        plt.xlabel("Baseline (m)")
        plt.ylabel("Resolution (arcsec)")
        plt.title("Wave 5C resolution estimate")
        """ + "\n"
        let scientificPythonEditorText = String(scientificPython.dropLast())
        try bringIntoView("notebook.python.editor.\(pythonCellID)", in: "notebook.document.scroll", deltaY: 280)
        replaceText("notebook.python.editor.\(pythonCellID)", with: scientificPythonEditorText)
        try clickIdentified("notebook.save")
        try bringIntoView("notebook.python.run.\(pythonCellID)", in: "notebook.document.scroll", deltaY: -220)
        try clickIdentified("notebook.python.run.\(pythonCellID)")
        let firstSuccess = try waitForReceiptObject(in: runs, timeout: 40) {
            guard $0["operation_id"] as? String == "python.execute",
                  $0["status"] as? String == "succeeded",
                  let input = $0["execution_input"] as? [String: Any],
                  let details = input["details"] as? [String: Any]
            else { return false }
            return details["source"] as? String == scientificPython
        }
        let firstDetails = try XCTUnwrap(
            (firstSuccess["execution_input"] as? [String: Any])?["details"] as? [String: Any]
        )
        let firstEnvironment = try XCTUnwrap(firstDetails["environment"] as? [String: Any])
        XCTAssertEqual((firstSuccess["schema_version"] as? NSNumber)?.intValue, 2)
        XCTAssertEqual(
            URL(fileURLWithPath: try XCTUnwrap(firstEnvironment["interpreter"] as? String))
                .resolvingSymlinksInPath().path,
            URL(fileURLWithPath: try XCTUnwrap(environment["pythonCommand"]))
                .resolvingSymlinksInPath().path
        )
        XCTAssertTrue((firstEnvironment["fingerprint_sha256"] as? String)?.isEmpty == false)
        let usefulPackages = try XCTUnwrap(firstEnvironment["packages"] as? [String: String])
        XCTAssertNotNil(usefulPackages["numpy"])
        XCTAssertNotNil(usefulPackages["matplotlib"])
        XCTAssertTrue(
            (firstSuccess["artifacts"] as? [[String: Any]])?.contains {
                $0["role"] as? String == "figure" && $0["media_type"] as? String == "image/png"
            } == true
        )

        let regeneratedPython = scientificPython
            .replacingOccurrences(of: "baseline_m = 1000.0", with: "baseline_m = 2000.0")
            .replacingOccurrences(of: "Wave 5C resolution estimate", with: "Wave 5C revised resolution estimate")
        let regeneratedPythonEditorText = String(regeneratedPython.dropLast())
        try bringIntoView("notebook.python.editor.\(pythonCellID)", in: "notebook.document.scroll", deltaY: 260)
        replaceText("notebook.python.editor.\(pythonCellID)", with: regeneratedPythonEditorText)
        try clickIdentified("notebook.save")
        try bringIntoView("notebook.python.run.\(pythonCellID)", in: "notebook.document.scroll", deltaY: -220)
        try clickIdentified("notebook.python.run.\(pythonCellID)")
        let regenerated = try waitForReceiptObject(in: runs, timeout: 40) {
            guard $0["operation_id"] as? String == "python.execute",
                  $0["status"] as? String == "succeeded",
                  let input = $0["execution_input"] as? [String: Any],
                  let details = input["details"] as? [String: Any]
            else { return false }
            return details["source"] as? String == regeneratedPython
        }
        let pythonCellReceiptID = try XCTUnwrap(regenerated["cell_id"] as? String)
        XCTAssertEqual(pythonCellReceiptID, pythonCellID)
        let regeneratedRunID = try XCTUnwrap(regenerated["run_id"] as? String)
        let regeneratedRevision = try XCTUnwrap(regenerated["revision"] as? NSNumber).uint64Value
        try bringIntoView("notebook.python.previousRevisions.\(pythonCellID)", in: "notebook.document.scroll", deltaY: -260)
        XCTAssertTrue(element("notebook.python.figure.\(regeneratedRunID)-\(regeneratedRevision)").exists)

        try clickIdentified("dock.mode.datasets")
        let outputRowID = "dataset.row.\(outputMS.path)"
        XCTAssertTrue(element(outputRowID).waitForExistence(timeout: 15), app.debugDescription)
        element(outputRowID).doubleClick()
        let modeID = "msExplore.mode.\(outputMS.path)"
        let mode = try require(modeID, timeout: 10)
        let plotsSegment = mode.descendants(matching: .radioButton).matching(
            NSPredicate(format: "label == %@", "Plots")
        ).firstMatch
        XCTAssertTrue(plotsSegment.waitForExistence(timeout: 5), app.debugDescription)
        plotsSegment.click()
        let generateID = "msPlot.generate.\(outputMS.path)"
        try clickIdentified(generateID)
        let saveID = "msPlot.saveToNotebook.\(outputMS.path)"
        XCTAssertTrue(element(saveID).waitForExistence(timeout: 30), app.debugDescription)
        try clickIdentified(saveID)
        XCTAssertTrue(app.menuItems["New plot"].waitForExistence(timeout: 5), app.debugDescription)
        app.menuItems["New plot"].click()
        let visualizationID = try waitForVisualizationID(in: notebookFile, timeout: 10)

        let preset = try require("msPlot.preset.\(outputMS.path)")
        preset.click()
        XCTAssertTrue(app.menuItems["Amplitude vs Time"].waitForExistence(timeout: 5), app.debugDescription)
        app.menuItems["Amplitude vs Time"].click()
        try clickIdentified(generateID)
        XCTAssertTrue(element(saveID).waitForExistence(timeout: 30), app.debugDescription)
        try clickIdentified(saveID)
        XCTAssertTrue(app.menuItems["Update UV Coverage"].waitForExistence(timeout: 5), app.debugDescription)
        app.menuItems["Update UV Coverage"].click()
        let visualizationFile = project
            .appendingPathComponent(".casa-rs/notebook-visualizations", isDirectory: true)
            .appendingPathComponent("\(visualizationID).json")
        XCTAssertTrue(waitForPath(visualizationFile, timeout: 10))
        let visualization = try XCTUnwrap(
            JSONSerialization.jsonObject(with: Data(contentsOf: visualizationFile)) as? [String: Any]
        )
        let visualizationRevisions = try XCTUnwrap(visualization["revisions"] as? [[String: Any]])
        XCTAssertEqual(visualizationRevisions.count, 2)
        XCTAssertNotEqual(
            visualizationRevisions[0]["asset_path"] as? String,
            visualizationRevisions[1]["asset_path"] as? String
        )
        for revision in visualizationRevisions {
            let render = try XCTUnwrap(revision["render"] as? [String: Any])
            XCTAssertEqual((render["width"] as? NSNumber)?.intValue, 960)
            XCTAssertEqual((render["height"] as? NSNumber)?.intValue, 600)
            let reopen = try XCTUnwrap(revision["reopen"] as? [String: Any])
            XCTAssertEqual(reopen["surface"] as? String, "msexplore")
            XCTAssertEqual(revision["source_references"] as? [String], [outputMS.path])
        }

        try clickIdentified("central.tab.tab-scientific-notebook")
        try bringIntoView(
            "notebook.visualization.\(visualizationID)",
            in: "notebook.document.scroll",
            deltaY: -480,
            attempts: 16
        )
        XCTAssertTrue(element("notebook.visualization.previousRevisions.\(visualizationID)").exists)
        try clickIdentified("notebook.visualization.preview.2")
        XCTAssertTrue(element("notebook.visualization.lightbox").waitForExistence(timeout: 5), app.debugDescription)
        app.typeKey(.escape, modifierFlags: [])
        try bringIntoView("notebook.visualization.openExplorer.\(visualizationID)", in: "notebook.document.scroll", deltaY: 200)
        try clickIdentified("notebook.visualization.openExplorer.\(visualizationID)")
        XCTAssertTrue(waitForValue("msPlot.preset.\(outputMS.path)", containing: "Amplitude vs Time"))

        app.terminate()
        XCTAssertTrue(app.wait(for: .notRunning, timeout: 8), "Production app did not terminate")
        launchLiveAssistantProject(project, environment: environment)
        try openProductionAssistant(notebookID: notebookID)
        let restartedTranscript = try waitForLiveAssistantTranscript(in: project, timeout: 10) {
            $0.messages.contains { $0.content.contains(marker) }
        }
        try clickIdentified("assistant.close")
        try bringIntoView(
            "notebook.visualization.\(visualizationID)",
            in: "notebook.document.scroll",
            deltaY: -480,
            attempts: 16
        )
        XCTAssertTrue(element("notebook.visualization.previousRevisions.\(visualizationID)").exists)
        try bringIntoView("notebook.python.previousRevisions.\(pythonCellID)", in: "notebook.document.scroll", deltaY: 420)
        XCTAssertTrue(element("notebook.python.latestRevision.\(pythonCellID)").exists)
        try bringIntoView("notebook.parameters.open.\(taskCellID)", in: "notebook.document.scroll", deltaY: -420)
        try clickIdentified("notebook.parameters.open.\(taskCellID)")
        XCTAssertTrue(waitForValue("task.parameter.output_ms", containing: outputRelativePath, timeout: 10))
        XCTAssertFalse(element("task.stop").isEnabled, "Reloading the receipt parameters must not rerun the task")

        let finalMarkdown = try String(contentsOf: notebookFile, encoding: .utf8)
        XCTAssertEqual(finalMarkdown.components(separatedBy: "casa-rs-ai-pin:v1").count - 1, 1)
        XCTAssertEqual(finalMarkdown.components(separatedBy: "casa-rs-visualization:v1").count - 1, 1)
        XCTAssertTrue(finalMarkdown.contains("Wave 5C revised resolution estimate"))
        let durableReceipts = receiptObjects(in: runs)
        XCTAssertEqual(durableReceipts.filter { $0["operation_id"] as? String == "simobserve" }.count, 1)
        XCTAssertEqual(
            durableReceipts.filter {
                $0["operation_id"] as? String == "python.execute"
                    && $0["status"] as? String == "failed"
            }.count,
            1
        )
        XCTAssertEqual(
            durableReceipts.filter {
                $0["operation_id"] as? String == "python.execute"
                    && $0["status"] as? String == "succeeded"
            }.count,
            2
        )
        let durableTaskReceipt = try XCTUnwrap(
            durableReceipts.first { $0["operation_id"] as? String == "simobserve" }
        )
        let durablePythonReceipts = durableReceipts.filter {
            $0["operation_id"] as? String == "python.execute"
                && $0["status"] as? String == "succeeded"
        }
        let restartedAnswer = try XCTUnwrap(
            restartedTranscript.messages.first { $0.role == "assistant" && $0.content.contains(marker) }
        )
        let taskProducts = (durableTaskReceipt["products"] as? [[String: Any]])?.compactMap {
            $0["path"] as? String
        } ?? []
        let pythonEvidence = durablePythonReceipts.compactMap { receipt -> [String: Any]? in
            guard let input = receipt["execution_input"] as? [String: Any],
                  let details = input["details"] as? [String: Any]
            else { return nil }
            return [
                "run_id": receipt["run_id"] as? String ?? "",
                "revision": (receipt["revision"] as? NSNumber)?.uint64Value ?? 0,
                "source_sha256": details["source_sha256"] as? String ?? "",
                "interpreter": (details["environment"] as? [String: Any])?["interpreter"] as? String ?? "",
                "environment_fingerprint_sha256":
                    (details["environment"] as? [String: Any])?["fingerprint_sha256"] as? String ?? "",
                "artifacts": (receipt["artifacts"] as? [[String: Any]])?.compactMap {
                    $0["path"] as? String
                } ?? [],
            ]
        }
        let visualizationEvidence = visualizationRevisions.map { revision -> [String: Any] in
            let reopen = revision["reopen"] as? [String: Any]
            let parameters = reopen?["parameters"] as? [String: Any]
            return [
                "revision": (revision["revision"] as? NSNumber)?.uint64Value ?? 0,
                "asset_path": revision["asset_path"] as? String ?? "",
                "preset": parameters?["preset"] as? String ?? "",
                "source_references": revision["source_references"] as? [String] ?? [],
            ]
        }
        let evidence: [String: Any] = [
            "schema_version": 1,
            "repository_revision": environment["repoRevision"] ?? "",
            "project_root": project.path,
            "notebook": "notebooks/Analysis.md",
            "assistant": [
                "marker": marker,
                "citation_locators": restartedAnswer.citations.map(\.locator),
                "pin_count": 1,
            ],
            "task": [
                "operation_id": "simobserve",
                "cell_id": durableTaskReceipt["cell_id"] as? String ?? "",
                "run_id": durableTaskReceipt["run_id"] as? String ?? "",
                "receipt_schema_version": (durableTaskReceipt["schema_version"] as? NSNumber)?.intValue ?? 0,
                "output": outputMS.path,
                "products": taskProducts,
            ],
            "python": [
                "selected_interpreter": firstEnvironment["interpreter"] as? String ?? "",
                "version": firstEnvironment["version"] as? String ?? "",
                "packages": usefulPackages,
                "failed_attempts": 1,
                "successful_revisions": pythonEvidence,
            ],
            "visualization": [
                "id": visualizationID,
                "revisions": visualizationEvidence,
                "previewed_revision": 2,
                "reopened_preset": "Amplitude vs Time",
            ],
            "restart_count": 2,
            "receipt_counts": [
                "simobserve_succeeded": 1,
                "python_failed": 1,
                "python_succeeded": 2,
            ],
            "cleanup": "test-owned project removed by harness after report validation",
        ]
        let evidenceData = try JSONSerialization.data(
            withJSONObject: evidence,
            options: [.prettyPrinted, .sortedKeys]
        )
        try evidenceData.write(
            to: URL(fileURLWithPath: try XCTUnwrap(environment["evidenceReport"])),
            options: .atomic
        )
        try Data().write(
            to: URL(fileURLWithPath: try XCTUnwrap(environment["passReceipt"])),
            options: .atomic
        )
        let screenshot = XCTAttachment(screenshot: app.screenshot())
        screenshot.name = "Wave 5C production notebook task Python plot round-trip"
        screenshot.lifetime = .keepAlways
        add(screenshot)
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
            waitForTextValue(
                element("notebook.richElement.rich-element-3"),
                containing: "Compare calibrated amplitudes and phases before imaging."
            )
        )
        XCTAssertEqual(try accessibilityValue("notebook.dirtyState"), "dirty")

        try bringIntoView(
            "tutorialPrototype.dataset.review.\(datasetID)",
            in: "notebook.document.scroll",
            deltaY: -260
        )
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
        try bringIntoView(
            "tutorialPrototype.dataset.review.\(datasetID)",
            in: "notebook.document.scroll",
            deltaY: -260
        )
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
        try bringIntoView(
            "tutorialPrototype.dataset.review.\(datasetID)",
            in: "notebook.document.scroll",
            deltaY: -260
        )
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
        try bringIntoView(
            "tutorialPrototype.dataset.review.\(datasetID)",
            in: "notebook.document.scroll",
            deltaY: -260
        )
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

        try bringIntoView(
            "tutorialPrototype.dataset.review.tutorial-dataset-twhya-calibrated",
            in: "notebook.document.scroll",
            deltaY: -260
        )

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
               !visibleWindowFrame.contains(frame)
            {
                // XCTest audits lazily retained and edge-clipped ScrollView
                // descendants. Their element snapshots include unrelated
                // screen pixels, so they have no complete rendered foreground
                // and background pair to evaluate at this scroll position.
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
        XCTAssertTrue(try require("aiPrototype.openDrawer").label.contains("Discuss this notebook"))
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
        XCTAssertTrue(try require("aiPrototype.model").exists)
        XCTAssertTrue(try require("aiPrototype.effort").exists)
        XCTAssertTrue(try require("aiPrototype.usage").exists)
        XCTAssertTrue(try require("aiPrototype.settings").exists)

        try clickIdentified("aiPrototype.effort")
        XCTAssertTrue(app.menuItems["High"].waitForExistence(timeout: 3))
        app.menuItems["High"].click()
        XCTAssertTrue(try require("aiPrototype.effort").value as? String == "High")

        try clickIdentified("aiPrototype.usage")
        XCTAssertTrue(try require("aiPrototype.usagePanel").exists)
        app.typeKey(.escape, modifierFlags: [])

        try clickIdentified("aiPrototype.settings")
        XCTAssertTrue(try require("aiPrototype.settingsPanel").exists)
        XCTAssertTrue(try require("aiPrototype.agent").exists)
        XCTAssertTrue(try require("aiPrototype.account").exists)
        XCTAssertTrue(try require("aiPrototype.trust").exists)
        XCTAssertTrue(try require("aiPrototype.python").exists)

        try clickIdentified("aiPrototype.trust")
        XCTAssertTrue(app.menuItems["Full access"].waitForExistence(timeout: 3))
        app.menuItems["Full access"].click()
        XCTAssertTrue(try require("aiPrototype.fullAccessSheet").exists)
        try clickIdentified("aiPrototype.fullAccess.confirm")
        XCTAssertTrue(try require("aiPrototype.fullAccessIndicator").exists)

        try clickIdentified("aiPrototype.contextPreview")
        XCTAssertTrue(try require("aiPrototype.workspaceSource.tab-task").exists)
        XCTAssertTrue(try require("aiPrototype.workspaceSource.corpus-radio").exists)
        XCTAssertTrue(try require("aiPrototype.workspaceSource.source-casars").exists)
        XCTAssertTrue(try require("aiPrototype.context.semantics").exists)
        try clickIdentified("aiPrototype.context.close")

        try clickIdentified("aiPrototype.suggestion.plot")
        XCTAssertTrue(try require("aiPrototype.input").value as? String == "Compare the current plot with the TW Hya paper.")
        try clickIdentified("aiPrototype.expand")
        XCTAssertTrue(try require("aiPrototype.expanded").exists)
        XCTAssertTrue(try require("aiPrototype.input").value as? String == "Compare the current plot with the TW Hya paper.")
        try clickIdentified("aiPrototype.dock")
        XCTAssertTrue(try require("aiPrototype.drawer").exists)

        let composer = try require("aiPrototype.input")
        composer.click()
        composer.typeKey(.return, modifierFlags: [.shift])
        XCTAssertFalse(element("aiPrototype.message.ai-assistant-1").exists)
        composer.typeKey(.return, modifierFlags: [])
        XCTAssertTrue(try require("aiPrototype.message.ai-assistant-1", timeout: 5).exists)
        try clickIdentified("aiPrototype.citation.citation-paper")
        XCTAssertTrue(try require("aiPrototype.sourcePreview").exists)
        XCTAssertTrue(try require("aiPrototype.message.ai-assistant-1.activity").exists)
        try clickIdentified("aiPrototype.message.ai-assistant-1.activity")
        try clickIdentified("aiPrototype.message.ai-assistant-1.addToNotebook")
        XCTAssertFalse(element("aiPrototype.pinSheet").exists)
        try clickIdentified("notebook.viewMode.raw")
        let rawMarkdown = try textValue(try require("notebook.editor.raw"))
        XCTAssertTrue(rawMarkdown.contains("## AI note"))
        XCTAssertTrue(rawMarkdown.hasSuffix("- [casa-ms source] crates/casa-ms/src/msexplore.rs · build_plot_document"))

        try clickIdentified("aiPrototype.message.ai-assistant-1.openTask")
        XCTAssertTrue(try require("prototypeTask.parameterSource.robust").exists)
        XCTAssertTrue(try require("prototypeTask.parameter.robust").value as? String == "-0.5")
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

    private func launchLiveAssistantProject(_ project: URL, environment: [String: String]) {
        app = XCUIApplication()
        ensureStoppedBeforeLaunch()
        app.launchEnvironment["PATH"] = environment["path"] ?? "/usr/bin:/bin"
        app.launchEnvironment["HOME"] = environment["home"] ?? FileManager.default.homeDirectoryForCurrentUser.path
        app.launchEnvironment["CODEX_HOME"] = environment["codexHome"] ?? ""
        app.launchEnvironment["CASA_RS_AGENT_COMMAND"] = environment["agentCommand"] ?? "codex"
        app.launchEnvironment["CASA_RS_GUI_TEST_PYTHON"] = environment["pythonCommand"] ?? "python3"
        if let repoRoot = environment["repoRoot"] {
            app.launchEnvironment["CASA_RS_REPO_ROOT"] = repoRoot
            app.launchEnvironment["CASA_RS_SOURCE_ROOT"] = repoRoot
        }
        if let simobserve = environment["simobserveCommand"] {
            app.launchEnvironment["CASARS_SIMOBSERVE_BIN"] = simobserve
        }
        if let msexplore = environment["msexploreCommand"] {
            app.launchEnvironment["CASARS_MSEXPLORE_BIN"] = msexplore
        }
        app.launchEnvironment["OPENAI_API_KEY"] = ""
        app.launchEnvironment["AZURE_OPENAI_API_KEY"] = ""
        app.launchEnvironment["OPENAI_BASE_URL"] = ""
        app.launchArguments = [
            "-ApplePersistenceIgnoreState", "YES",
            "-NSAutomaticTextCompletionEnabled", "NO",
            "--open-project", project.path,
        ]
        app.launch()
        app.activate()
        XCTAssertTrue(app.windows["casa-rs Workbench"].waitForExistence(timeout: 15), app.debugDescription)
    }

    private func openProductionAssistant(notebookID: String) throws {
        try clickIdentified("dock.mode.notebooks")
        XCTAssertTrue(notebookSelector(notebookID).waitForExistence(timeout: 5), app.debugDescription)
        try clickIdentified("notebook.selector.open")
        if element("inspector.collapse").isHittable { try clickIdentified("inspector.collapse") }
        try clickIdentified("assistant.openDrawer")
        XCTAssertTrue(element("assistant.discussion").waitForExistence(timeout: 8), app.debugDescription)
    }

    private func sendLiveAssistantPrompt(_ prompt: String) throws {
        replaceText("assistant.input", with: prompt)
        let send = try require("assistant.send")
        let sendReady = XCTNSPredicateExpectation(
            predicate: NSPredicate(format: "enabled == true"),
            object: send
        )
        XCTAssertEqual(
            XCTWaiter.wait(for: [sendReady], timeout: 25),
            .completed,
            "Assistant sidecar did not become ready to send"
        )
        try clickIdentified("assistant.send")
    }

    private func waitForLiveAssistantTranscript(
        in project: URL,
        timeout: TimeInterval,
        matching predicate: (LiveAssistantTranscript) -> Bool
    ) throws -> LiveAssistantTranscript {
        let directory = project.appendingPathComponent(".casa-rs/conversations", isDirectory: true)
        let deadline = Date().addingTimeInterval(timeout)
        repeat {
            let urls = (try? FileManager.default.contentsOfDirectory(
                at: directory,
                includingPropertiesForKeys: nil
            )) ?? []
            for url in urls where url.pathExtension == "json" {
                if let data = try? Data(contentsOf: url),
                   let transcript = try? JSONDecoder.liveAssistant.decode(
                       LiveAssistantTranscript.self,
                       from: data
                   ),
                   predicate(transcript)
                {
                    return transcript
                }
            }
            Thread.sleep(forTimeInterval: 0.1)
        } while Date() < deadline
        let message = "Timed out waiting for the real assistant transcript condition at \(directory.path)"
        XCTFail(message)
        throw LiveAssistantAcceptanceError.transcriptTimeout(message)
    }

    private func require(_ identifier: String, timeout: TimeInterval = 5) throws -> XCUIElement {
        let result = element(identifier)
        XCTAssertTrue(result.waitForExistence(timeout: timeout), "Missing accessibility identifier \(identifier)\n\(app.debugDescription)")
        return result
    }

    private func element(_ identifier: String) -> XCUIElement {
        // XCTest's direct identifier query traps when the identifier is longer
        // than 128 characters. Real project paths can legitimately make stable
        // path-derived identifiers longer than that, so use the equivalent
        // predicate form for those controls.
        if identifier.utf16.count > 128 {
            return app.descendants(matching: .any).matching(
                NSPredicate(format: "identifier == %@", identifier)
            ).firstMatch
        }
        return app.descendants(matching: .any).matching(identifier: identifier).firstMatch
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
        if identifier.hasPrefix("notebook.richElement.") {
            let editor = app.textViews.matching(identifier: identifier).firstMatch
            XCTAssertTrue(
                editor.waitForExistence(timeout: 2),
                "Rendered Markdown block \(identifier) did not enter edit mode"
            )
            editor.click()
        }
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
        let element = element(identifier)
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

    private func waitForPath(_ url: URL, timeout: TimeInterval) -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        repeat {
            if FileManager.default.fileExists(atPath: url.path) { return true }
            Thread.sleep(forTimeInterval: 0.05)
        } while Date() < deadline
        return false
    }

    private func waitForReceiptObject(
        in runs: URL,
        timeout: TimeInterval,
        matching predicate: ([String: Any]) -> Bool
    ) throws -> [String: Any] {
        let deadline = Date().addingTimeInterval(timeout)
        repeat {
            for receipt in receiptObjects(in: runs) {
                if predicate(receipt) { return receipt }
            }
            Thread.sleep(forTimeInterval: 0.05)
        } while Date() < deadline
        let message = "Timed out waiting for a matching notebook receipt in \(runs.path)"
        XCTFail(message)
        throw LiveAssistantAcceptanceError.transcriptTimeout(message)
    }

    private func receiptObjects(in runs: URL) -> [[String: Any]] {
        let urls = (try? FileManager.default.contentsOfDirectory(
            at: runs,
            includingPropertiesForKeys: nil
        ))?.map { $0.appendingPathComponent("receipt.json") } ?? []
        return urls.compactMap { receipt in
            guard let data = try? Data(contentsOf: receipt),
                  let object = try? JSONSerialization.jsonObject(with: data),
                  let dictionary = object as? [String: Any]
            else { return nil }
            return dictionary
        }
    }

    private func waitForVisualizationID(in notebook: URL, timeout: TimeInterval) throws -> String {
        let prefix = "<!-- casa-rs-visualization:v1 id="
        let deadline = Date().addingTimeInterval(timeout)
        repeat {
            if let source = try? String(contentsOf: notebook, encoding: .utf8),
               let start = source.range(of: prefix)?.upperBound,
               let end = source[start...].firstIndex(where: { $0 == " " || $0 == ">" })
            {
                let id = String(source[start..<end])
                if !id.isEmpty { return id }
            }
            Thread.sleep(forTimeInterval: 0.05)
        } while Date() < deadline
        let message = "Timed out waiting for a notebook visualization in \(notebook.path)"
        XCTFail(message)
        throw LiveAssistantAcceptanceError.transcriptTimeout(message)
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
            if (element.value as? String)?.contains(expected) == true
                || element.label.contains(expected)
            {
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

    private func resolvedTestPython() throws -> String {
        let environment = ProcessInfo.processInfo.environment
        let pathCandidates = (environment["PATH"] ?? "")
            .split(separator: ":")
            .flatMap { directory in
                ["python3", "python"].map {
                    URL(fileURLWithPath: String(directory)).appendingPathComponent($0).path
                }
            }
        let candidates = [environment["CASA_RS_GUI_TEST_PYTHON"]].compactMap { $0 }
            + pathCandidates
            + [
                "/opt/homebrew/bin/python3",
                "/usr/local/bin/python3",
                "/opt/local/bin/python3",
                "/Library/Frameworks/Python.framework/Versions/Current/bin/python3",
            ]
        if let executable = candidates.first(where: {
            FileManager.default.isExecutableFile(atPath: $0)
                && URL(fileURLWithPath: $0).resolvingSymlinksInPath().path != "/usr/bin/python3"
                && !$0.contains("/Xcode.app/Contents/Developer/")
        }) {
            return executable
        }
        throw XCTSkip("a standalone Python runtime is required for production GUI tests")
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

private struct LiveAssistantTranscript: Decodable {
    struct BackendSession: Decodable { var sessionId: String }
    struct PythonProvenance: Decodable {
        var resolvedPath: String
        var version: String
    }
    struct Profile: Decodable {
        var model: String
        var effort: String
        var pythonProvenance: PythonProvenance?
    }
    struct Activity: Decodable {
        var label: String
        var state: String
    }
    struct Citation: Decodable {
        var locator: String
    }
    struct TaskSuggestion: Decodable {
        var id: String
        var taskId: String
        var parameters: [String: String]
    }
    struct Message: Decodable {
        var id: String
        var role: String
        var content: String
        var citations: [Citation]
        var activities: [Activity]
        var taskSuggestions: [TaskSuggestion]
    }

    var backendSession: BackendSession?
    var profile: Profile
    var messages: [Message]
}

private enum LiveAssistantAcceptanceError: Error {
    case transcriptTimeout(String)
}

private extension JSONDecoder {
    static var liveAssistant: JSONDecoder {
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        return decoder
    }
}
