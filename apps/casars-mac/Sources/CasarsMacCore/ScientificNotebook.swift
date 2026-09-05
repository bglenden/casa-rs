import Foundation

/// Fixture-only prototype families exposed to the native app target.
///
/// These types are projections for interaction review. They are deliberately
/// prefixed with `Prototype`, are never written to disk, and are not notebook,
/// task-provider, or parameter-profile contracts.
package enum WorkbenchPrototypeKind: String, Codable, Equatable {
    case notebook
    case python
    case tutorial
    case ai
}

package enum NotebookPrototypeScenario: String, Codable, Equatable {
    case primary = "happy-path"
    case externalConflict = "external-conflict"
}

package enum PrototypeNotebookViewMode: String, CaseIterable, Codable, Equatable, Identifiable {
    case rich
    case raw

    package var id: String { rawValue }
}

package enum PrototypeNotebookReceiptStatus: String, Codable, Equatable {
    case running
    case succeeded
    case failed
    case cancelled
}

/// Read-only display value carried by the deterministic fixture adapter.
/// It is intentionally independent of provider parameter schemas.
package struct PrototypeNotebookParameterRow: Identifiable, Codable, Equatable {
    package var id: String { parameterID }
    package var parameterID: String
    package var label: String
    package var value: String

    package init(parameterID: String, label: String, value: String) {
        self.parameterID = parameterID
        self.label = label
        self.value = value
    }
}

package struct PrototypeNotebookExecutionRevision: Identifiable, Codable, Equatable {
    package let id: String
    package var sequence: Int
    package var timestamp: String
    package var status: PrototypeNotebookReceiptStatus
    package var summary: String
    package var products: [String]
    package var diagnostics: [String]
    package var logLines: [String]

    package init(
        id: String,
        sequence: Int,
        timestamp: String,
        status: PrototypeNotebookReceiptStatus,
        summary: String,
        products: [String] = [],
        diagnostics: [String] = [],
        logLines: [String] = []
    ) {
        self.id = id
        self.sequence = sequence
        self.timestamp = timestamp
        self.status = status
        self.summary = summary
        self.products = products
        self.diagnostics = diagnostics
        self.logLines = logLines
    }
}

/// Fixture projection for one inline task block in a prototype notebook.
package struct PrototypeNotebookTaskProjection: Identifiable, Codable, Equatable {
    package let id: String
    package var taskID: String
    package var title: String
    package var annotation: String
    package var contractVersion: UInt64
    package var sparseProfileTOML: String
    package var parameterRows: [PrototypeNotebookParameterRow]
    package var revisions: [PrototypeNotebookExecutionRevision]

    package init(
        id: String,
        taskID: String,
        title: String,
        annotation: String,
        contractVersion: UInt64,
        sparseProfileTOML: String,
        parameterRows: [PrototypeNotebookParameterRow],
        revisions: [PrototypeNotebookExecutionRevision]
    ) {
        self.id = id
        self.taskID = taskID
        self.title = title
        self.annotation = annotation
        self.contractVersion = contractVersion
        self.sparseProfileTOML = sparseProfileTOML
        self.parameterRows = parameterRows
        self.revisions = revisions
    }

    package var latestRevision: PrototypeNotebookExecutionRevision? {
        revisions.max { $0.sequence < $1.sequence }
    }
}

package struct PrototypeNotebookSummary: Identifiable, Codable, Equatable {
    package let id: String
    package var title: String
    package var filename: String
    package var displayPath: String

    package init(id: String, title: String, filename: String, displayPath: String) {
        self.id = id
        self.title = title
        self.filename = filename
        self.displayPath = displayPath
    }
}

/// Mutable in-memory projection for a single selectable prototype document.
package struct PrototypeNotebookDocumentProjection: Identifiable, Codable, Equatable {
    package let id: String
    package var title: String
    package var filename: String
    package var displayPath: String
    package var viewMode: PrototypeNotebookViewMode
    package var savedMarkdown: String
    package var draftMarkdown: String
    package var hasExternalConflict: Bool
    package var tasks: [PrototypeNotebookTaskProjection]
    package var selectedReceiptID: String?

    package init(
        id: String,
        title: String,
        filename: String,
        displayPath: String,
        viewMode: PrototypeNotebookViewMode = .rich,
        savedMarkdown: String,
        draftMarkdown: String,
        hasExternalConflict: Bool,
        tasks: [PrototypeNotebookTaskProjection],
        selectedReceiptID: String? = nil
    ) {
        self.id = id
        self.title = title
        self.filename = filename
        self.displayPath = displayPath
        self.viewMode = viewMode
        self.savedMarkdown = savedMarkdown
        self.draftMarkdown = draftMarkdown
        self.hasExternalConflict = hasExternalConflict
        self.tasks = tasks
        self.selectedReceiptID = selectedReceiptID
    }

    package var summary: PrototypeNotebookSummary {
        PrototypeNotebookSummary(id: id, title: title, filename: filename, displayPath: displayPath)
    }

    package var isDirty: Bool { draftMarkdown != savedMarkdown }

    package var selectedReceipt: PrototypeNotebookTaskProjection? {
        guard let selectedReceiptID else { return nil }
        return tasks.first { $0.id == selectedReceiptID }
    }
}

/// Aggregate fixture projection used only by the package-scoped prototype path.
package struct PrototypeScientificNotebookProjection: Codable, Equatable {
    package var prototypeKind: WorkbenchPrototypeKind
    package var scenario: NotebookPrototypeScenario
    package var documents: [PrototypeNotebookDocumentProjection]
    package var activeNotebookID: String
    package var nextSimulatedRunSequence: Int

    package init(
        prototypeKind: WorkbenchPrototypeKind = .notebook,
        scenario: NotebookPrototypeScenario,
        documents: [PrototypeNotebookDocumentProjection],
        activeNotebookID: String,
        nextSimulatedRunSequence: Int = 1
    ) {
        self.prototypeKind = prototypeKind
        self.scenario = scenario
        self.documents = documents
        self.activeNotebookID = activeNotebookID
        self.nextSimulatedRunSequence = nextSimulatedRunSequence
    }

    package var notebooks: [PrototypeNotebookSummary] { documents.map(\.summary) }

    package var activeDocument: PrototypeNotebookDocumentProjection? {
        documents.first { $0.id == activeNotebookID }
    }

    package var notebookID: String { activeDocument?.id ?? "" }
    package var title: String { activeDocument?.title ?? "" }
    package var filename: String { activeDocument?.filename ?? "" }
    package var displayPath: String { activeDocument?.displayPath ?? "" }
    package var viewMode: PrototypeNotebookViewMode { activeDocument?.viewMode ?? .rich }
    package var savedMarkdown: String { activeDocument?.savedMarkdown ?? "" }
    package var draftMarkdown: String { activeDocument?.draftMarkdown ?? "" }
    package var hasExternalConflict: Bool { activeDocument?.hasExternalConflict ?? false }
    package var receipts: [PrototypeNotebookTaskProjection] { activeDocument?.tasks ?? [] }
    package var selectedReceiptID: String? { activeDocument?.selectedReceiptID }
    package var selectedReceipt: PrototypeNotebookTaskProjection? { activeDocument?.selectedReceipt }
    package var isDirty: Bool { activeDocument?.isDirty ?? false }

    package func task(receiptID: String) -> PrototypeNotebookTaskProjection? {
        documents.lazy.flatMap(\.tasks).first { $0.id == receiptID }
    }
}

/// Deterministic fixture adapter for the Wave 1 interaction gate.
///
/// It performs no file, provider, schema, parameter, dataset, or task access.
package enum PrototypeScientificNotebookFixtureAdapter {
    package static func make(scenario: NotebookPrototypeScenario) -> PrototypeScientificNotebookProjection {
        let imagerProfile = """
        [casars]
        format = 1
        surface = "imager"
        kind = "task"
        contract = 15

        [parameters]
        vis = "data/twhya_calibrated.ms"
        imagename = "products/twhya-mfs"
        imsize = 1024
        cell = "1arcsec"
        niter = 1000
        """
        let imagerRows = [
            PrototypeNotebookParameterRow(parameterID: "vis", label: "MeasurementSet", value: "data/twhya_calibrated.ms"),
            PrototypeNotebookParameterRow(parameterID: "imagename", label: "Image name", value: "products/twhya-mfs"),
            PrototypeNotebookParameterRow(parameterID: "imsize", label: "Image size", value: "1024"),
            PrototypeNotebookParameterRow(parameterID: "cell", label: "Cell size", value: "1arcsec"),
            PrototypeNotebookParameterRow(parameterID: "niter", label: "Iterations", value: "1000"),
        ]
        let completed = PrototypeNotebookTaskProjection(
            id: "receipt-imager-mfs",
            taskID: "imager",
            title: "Create TW Hya MFS image",
            annotation: "Use the calibrated continuum data for a reproducible reference image.",
            contractVersion: 15,
            sparseProfileTOML: imagerProfile,
            parameterRows: imagerRows,
            revisions: [
                PrototypeNotebookExecutionRevision(
                    id: "execution-imager-mfs-1",
                    sequence: 1,
                    timestamp: "2026-07-10 10:14 MDT",
                    status: .succeeded,
                    summary: "Created the MFS image and associated weight products.",
                    products: ["products/twhya-mfs.image", "products/twhya-mfs.weight"],
                    diagnostics: ["Prototype receipt: no task was executed."],
                    logLines: [
                        "Resolved sparse imager parameters.",
                        "Simulated 1,000 clean iterations.",
                        "Registered two fixture products.",
                    ]
                )
            ]
        )
        let failed = PrototypeNotebookTaskProjection(
            id: "receipt-impbcor-failed",
            taskID: "impbcor",
            title: "Apply primary-beam correction",
            annotation: "Correct the reference image before measuring off-axis flux density.",
            contractVersion: 1,
            sparseProfileTOML: """
            [casars]
            format = 1
            surface = "impbcor"
            kind = "task"
            contract = 1

            [parameters]
            imagename = "products/twhya-mfs.image"
            pbimage = "products/twhya-mfs.pb"
            outfile = "products/twhya-mfs.pbcor"
            """,
            parameterRows: [
                PrototypeNotebookParameterRow(parameterID: "imagename", label: "Image", value: "products/twhya-mfs.image"),
                PrototypeNotebookParameterRow(parameterID: "pbimage", label: "Primary beam", value: "products/twhya-mfs.pb"),
                PrototypeNotebookParameterRow(parameterID: "outfile", label: "Output", value: "products/twhya-mfs.pbcor"),
            ],
            revisions: [
                PrototypeNotebookExecutionRevision(
                    id: "execution-impbcor-1",
                    sequence: 1,
                    timestamp: "2026-07-10 10:22 MDT",
                    status: .failed,
                    summary: "Primary-beam input was unavailable.",
                    diagnostics: ["Prototype failure state: products/twhya-mfs.pb was not staged."],
                    logLines: ["Validated request.", "Stopped before writing output."]
                )
            ]
        )
        let cancelled = PrototypeNotebookTaskProjection(
            id: "receipt-imager-cancelled",
            taskID: "imager",
            title: "Try alternate robust weighting",
            annotation: "Compare resolution and sensitivity after the reference image is accepted.",
            contractVersion: 15,
            sparseProfileTOML: imagerProfile + "\nrobust = -0.5",
            parameterRows: imagerRows + [
                PrototypeNotebookParameterRow(parameterID: "robust", label: "Robust", value: "-0.5")
            ],
            revisions: [
                PrototypeNotebookExecutionRevision(
                    id: "execution-imager-cancelled-1",
                    sequence: 1,
                    timestamp: "2026-07-10 10:30 MDT",
                    status: .cancelled,
                    summary: "User cancelled the trial before products were registered.",
                    diagnostics: ["Prototype cancellation state: no task was executed."],
                    logLines: ["Simulated run started.", "Cancellation acknowledged."]
                )
            ]
        )
        let analysisMarkdown = """
        # TW Hya reduction notes

        The calibrated continuum data look healthy. I will make a first MFS
        image, inspect the primary-beam coverage, and then compare weighting.

        ## Imaging intent

        Keep this first pass conservative so its products can serve as the
        reference for later experiments.

        Use the calibrated continuum data for a reproducible reference image.

        \(taskCell(id: completed.id, profileTOML: completed.sparseProfileTOML))

        Correct the reference image before measuring off-axis flux density.

        \(taskCell(id: failed.id, profileTOML: failed.sparseProfileTOML))

        Compare resolution and sensitivity after the reference image is accepted.

        \(taskCell(id: cancelled.id, profileTOML: cancelled.sparseProfileTOML))
        """
        let analysis = PrototypeNotebookDocumentProjection(
            id: "notebook-twhya-analysis",
            title: "TW Hya Analysis",
            filename: "Analysis.md",
            displayPath: "notebooks/Analysis.md",
            savedMarkdown: analysisMarkdown,
            draftMarkdown: scenario == .externalConflict
                ? analysisMarkdown + "\n\nLocal unsaved note: compare the robust-weighting runs."
                : analysisMarkdown,
            hasExternalConflict: scenario == .externalConflict,
            tasks: [completed, failed, cancelled],
            selectedReceiptID: completed.id
        )

        let listobs = PrototypeNotebookTaskProjection(
            id: "receipt-listobs-summary",
            taskID: "listobs",
            title: "Summarize the observation",
            annotation: "Capture the scan and antenna inventory next to the observing notes.",
            contractVersion: 1,
            sparseProfileTOML: """
            [casars]
            format = 1
            surface = "listobs"
            kind = "task"
            contract = 1

            [parameters]
            vis = "data/twhya_calibrated.ms"
            verbose = true
            """,
            parameterRows: [
                PrototypeNotebookParameterRow(parameterID: "vis", label: "MeasurementSet", value: "data/twhya_calibrated.ms"),
                PrototypeNotebookParameterRow(parameterID: "verbose", label: "Verbose", value: "true"),
            ],
            revisions: [
                PrototypeNotebookExecutionRevision(
                    id: "execution-listobs-1",
                    sequence: 1,
                    timestamp: "2026-07-10 09:48 MDT",
                    status: .succeeded,
                    summary: "Recorded 8 scans, 43 antennas, and 2 spectral windows.",
                    diagnostics: ["Prototype receipt: no MeasurementSet was opened."],
                    logLines: ["Read fixture observation summary.", "Rendered fixture scan inventory."]
                )
            ]
        )
        let observationMarkdown = """
        # Observation log

        The 2026-07-09 continuum execution completed under stable weather.
        Antennas DA42 and DV18 need a closer look before calibration.

        ## Next check

        Review the scan summary alongside the observer log before flagging.

        Capture the scan and antenna inventory next to the observing notes.

        \(taskCell(id: listobs.id, profileTOML: listobs.sparseProfileTOML))
        """
        let observationLog = PrototypeNotebookDocumentProjection(
            id: "notebook-twhya-observation-log",
            title: "TW Hya Observation Log",
            filename: "Observation Log.md",
            displayPath: "notebooks/Observation Log.md",
            savedMarkdown: observationMarkdown,
            draftMarkdown: observationMarkdown,
            hasExternalConflict: false,
            tasks: [listobs],
            selectedReceiptID: listobs.id
        )

        return PrototypeScientificNotebookProjection(
            scenario: scenario,
            documents: [analysis, observationLog],
            activeNotebookID: analysis.id
        )
    }

    private static func taskCell(id: String, profileTOML: String) -> String {
        """
        <!-- casa-rs-cell:v1 id=\(id) kind=task -->
        ```toml
        \(profileTOML)
        ```
        <!-- /casa-rs-cell -->
        """
    }

    /// Reprojects fixture task parameters from the same Markdown source edited
    /// in Raw mode. In production this mapping belongs to the Rust notebook and
    /// provider-contract layers; the prototype keeps only a bounded TOML parser.
    package static func synchronizeTaskCells(
        in document: inout PrototypeNotebookDocumentProjection
    ) {
        guard let projection = try? PrototypeNotebookRichProjectionAdapter.projection(
            markdown: document.draftMarkdown
        ) else { return }
        for index in document.tasks.indices {
            let taskID = document.tasks[index].id
            guard let cell = projection.cell(id: taskID),
                  cell.kind == "task",
                  let profileTOML = cell.bodySource(in: projection.source),
                  let rows = parameterRows(
                      profileTOML: profileTOML,
                      existing: document.tasks[index].parameterRows
                  )
            else { continue }
            document.tasks[index].sparseProfileTOML = profileTOML
            document.tasks[index].parameterRows = rows
        }
    }

    private static func parameterRows(
        profileTOML: String,
        existing: [PrototypeNotebookParameterRow]
    ) -> [PrototypeNotebookParameterRow]? {
        let labels = Dictionary(uniqueKeysWithValues: existing.map { ($0.parameterID, $0.label) })
        var foundParameters = false
        var inParameters = false
        var rows: [PrototypeNotebookParameterRow] = []
        for line in profileTOML.components(separatedBy: .newlines) {
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed == "[parameters]" {
                foundParameters = true
                inParameters = true
                continue
            }
            if trimmed.hasPrefix("[") {
                inParameters = false
                continue
            }
            guard inParameters,
                  !trimmed.isEmpty,
                  !trimmed.hasPrefix("#"),
                  let separator = trimmed.firstIndex(of: "=")
            else { continue }
            let parameterID = trimmed[..<separator]
                .trimmingCharacters(in: .whitespacesAndNewlines)
            var value = trimmed[trimmed.index(after: separator)...]
                .trimmingCharacters(in: .whitespacesAndNewlines)
            if value.count >= 2, value.first == "\"", value.last == "\"" {
                value.removeFirst()
                value.removeLast()
            }
            guard !parameterID.isEmpty else { continue }
            rows.append(
                PrototypeNotebookParameterRow(
                    parameterID: parameterID,
                    label: labels[parameterID] ?? parameterID,
                    value: value
                )
            )
        }
        return foundParameters ? rows : nil
    }
}
