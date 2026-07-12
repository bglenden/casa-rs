import AppKit
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

        XCTAssertTrue(
            app.staticTexts["error: RuntimeError: fixture: channel selection is empty"]
                .waitForExistence(timeout: 5)
        )
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
        documentScroll.scroll(byDeltaX: 0, deltaY: -180)
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
            withDestinationURL: URL(fileURLWithPath: "/usr/bin/python3")
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
        let visibleOutput = app.staticTexts
            .matching(identifier: "notebook.python.cell.\(cellID)")
            .matching(NSPredicate(format: "label == %@", "42"))
            .firstMatch
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
        XCTAssertTrue(
            app.staticTexts
                .matching(identifier: "notebook.python.cell.\(cellID)")
                .matching(NSPredicate(format: "label == %@", "42"))
                .firstMatch
                .waitForExistence(timeout: 5)
        )
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
        let scroll = app.scrollViews[scrollIdentifier]
        XCTAssertTrue(scroll.waitForExistence(timeout: 5), "Missing scroll view \(scrollIdentifier)")
        XCTAssertTrue(scroll.isHittable, "Scroll view is not hittable: \(scrollIdentifier)")
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
        segment.click()
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

    private func waitForAccessibilityValue(_ identifier: String, containing substring: String) -> Bool {
        waitForValue(identifier, containing: substring)
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
            || value == "TW Hya · continuum image"
            || value == "Code"
            || value == "88d14db8cc92074d"
            || value.hasPrefix("from casars import msexplore\n")
    }
}
