import Foundation

/// Fixture-only Wave 3 scenarios. These values are accepted by the prototype
/// CLI and never select a production dataset or acquisition adapter.
package enum TutorialNotebookPrototypeScenario: String, CaseIterable, Codable, Equatable {
    case happyPath = "happy-path"
    case checksumFailure = "checksum-failure"
    case diskFailure = "disk-failure"
    case offline
    case unsafeArchive = "unsafe-archive"
}

package enum TutorialNotebookSectionStatus: String, Codable, Equatable {
    case notStarted = "not-started"
    case inProgress = "in-progress"
    case completed
    case blocked
}

package struct TutorialNotebookSectionProjection: Identifiable, Codable, Equatable {
    package let id: String
    package var title: String
    package var status: TutorialNotebookSectionStatus
}

package enum TutorialNotebookAcquisitionPhase: String, Codable, Equatable {
    case missing
    case approvalRequired = "approval-required"
    case downloading
    case verifying
    case unpacking
    case ready
    case cancelled
    case checksumFailed = "checksum-failed"
    case diskFailed = "disk-failed"
    case offline
    case unsafeArchive = "unsafe-archive"

    package var isRunning: Bool {
        switch self {
        case .downloading, .verifying, .unpacking:
            true
        case .missing, .approvalRequired, .ready, .cancelled, .checksumFailed,
             .diskFailed, .offline, .unsafeArchive:
            false
        }
    }
}

package struct TutorialNotebookOptionalCheck: Identifiable, Codable, Equatable {
    package let id: String
    package var label: String
    package var isEnabled: Bool
}

/// Facts shown before the fixture acquisition can begin.
package struct TutorialNotebookApprovalFacts: Codable, Equatable {
    package var scheme: String
    package var requestedURL: String
    package var resolvedURL: String
    package var redirects: [String]
    package var expectedSizeBytes: UInt64
    package var destination: String
    package var expectedSHA256: String
    package var requiredDiskBytes: UInt64
    package var freeDiskBytes: UInt64
    package var extractionPlan: String
    package var optionalChecks: [TutorialNotebookOptionalCheck]

    package var hasEnoughDisk: Bool { freeDiskBytes >= requiredDiskBytes }
}

package enum TutorialNotebookAttemptKind: String, Codable, Equatable {
    case initial
    case resume
    case restart
    case retry
    case makeSpaceRetry = "make-space-retry"
}

/// One immutable-generation view of the deterministic acquisition state.
package struct TutorialNotebookDatasetAttempt: Identifiable, Codable, Equatable {
    package var id: Int { generation }
    package let generation: Int
    package var kind: TutorialNotebookAttemptKind
    package var phase: TutorialNotebookAcquisitionPhase
    package var resumeOffsetBytes: UInt64
    package var downloadedBytes: UInt64
    package var expectedBytes: UInt64
    package var message: String?

    package var progress: Double {
        guard expectedBytes > 0 else { return 0 }
        return min(1, Double(downloadedBytes) / Double(expectedBytes))
    }
}

package struct TutorialNotebookDatasetProjection: Identifiable, Codable, Equatable {
    package let id: String
    package var name: String
    package var archiveName: String
    package var destination: String
    package var phase: TutorialNotebookAcquisitionPhase
    package var isStaged: Bool
    package var currentGeneration: Int
    package var attempts: [TutorialNotebookDatasetAttempt]
    package var message: String?

    package var currentAttempt: TutorialNotebookDatasetAttempt? {
        attempts.last { $0.generation == currentGeneration }
    }

    package var isReady: Bool { phase == .ready && isStaged }
}

/// Mutable, in-memory-only Wave 3 tutorial interaction projection.
///
/// The nested notebook and task use the existing Wave 1 fixture projections so
/// the tutorial prototype does not establish a second notebook or task shape.
package struct TutorialNotebookPrototypeProjection: Codable, Equatable {
    package var prototypeKind: WorkbenchPrototypeKind
    package var scenario: TutorialNotebookPrototypeScenario
    package var title: String
    package var sections: [TutorialNotebookSectionProjection]
    package var selectedSectionID: String
    package var dataset: TutorialNotebookDatasetProjection
    package var approvalFacts: TutorialNotebookApprovalFacts
    package var activeApproval: TutorialNotebookApprovalFacts?
    package var learnerNotebook: PrototypeScientificNotebookProjection
    package var fixtureTask: PrototypeNotebookTaskProjection
    package var scenarioFailureConsumed: Bool

    package var selectedSection: TutorialNotebookSectionProjection? {
        sections.first { $0.id == selectedSectionID }
    }

    package var currentGeneration: Int { dataset.currentGeneration }

    @discardableResult
    package mutating func showApproval() -> Bool {
        guard dataset.phase == .missing else { return false }
        transition(to: .approvalRequired)
        activeApproval = approvalFacts
        return true
    }

    @discardableResult
    package mutating func dismissApproval() -> Bool {
        guard dataset.phase == .approvalRequired else { return false }
        activeApproval = nil
        transition(to: .missing)
        return true
    }

    /// Approves only the displayed facts. Opening the tutorial never calls this
    /// method, which keeps acquisition explicit.
    @discardableResult
    package mutating func approve() -> Int? {
        guard dataset.phase == .approvalRequired, activeApproval != nil else { return nil }
        activeApproval = nil
        let generation = beginAttempt(kind: .initial, resumeOffsetBytes: 0)
        if !approvalFacts.hasEnoughDisk {
            scenarioFailureConsumed = true
            transition(
                to: .diskFailed,
                message: "Not enough project-local disk space for download and safe extraction."
            )
        }
        return generation
    }

    /// Advances one deterministic fixture step and rejects callbacks from an
    /// obsolete attempt generation.
    @discardableResult
    package mutating func advance(generation: Int) -> Bool {
        guard generation == dataset.currentGeneration,
              let attempt = dataset.currentAttempt,
              attempt.phase.isRunning
        else { return false }

        switch attempt.phase {
        case .downloading:
            if scenario == .offline, !scenarioFailureConsumed {
                scenarioFailureConsumed = true
                let partial = max(attempt.downloadedBytes, approvalFacts.expectedSizeBytes / 4)
                transition(
                    to: .offline,
                    downloadedBytes: partial,
                    message: "The network became unavailable; the verified partial download can be resumed."
                )
            } else if attempt.downloadedBytes < approvalFacts.expectedSizeBytes / 2 {
                transition(
                    to: .downloading,
                    downloadedBytes: approvalFacts.expectedSizeBytes / 2
                )
            } else {
                transition(
                    to: .verifying,
                    downloadedBytes: approvalFacts.expectedSizeBytes
                )
            }
        case .verifying:
            if scenario == .checksumFailure, !scenarioFailureConsumed {
                scenarioFailureConsumed = true
                transition(
                    to: .checksumFailed,
                    message: "SHA-256 did not match the approved TW Hya archive digest."
                )
            } else {
                transition(to: .unpacking)
            }
        case .unpacking:
            if scenario == .unsafeArchive, !scenarioFailureConsumed {
                scenarioFailureConsumed = true
                transition(
                    to: .unsafeArchive,
                    message: "Safe extraction rejected an archive member that escaped the destination."
                )
            } else {
                transition(to: .ready, message: "Verified fixture dataset is ready in the project.")
                unlockDatasetSections()
            }
        case .missing, .approvalRequired, .ready, .cancelled, .checksumFailed,
             .diskFailed, .offline, .unsafeArchive:
            return false
        }
        return true
    }

    @discardableResult
    package mutating func cancel() -> Bool {
        guard dataset.phase.isRunning else { return false }
        transition(to: .cancelled, message: "Acquisition cancelled; no dataset was staged.")
        return true
    }

    @discardableResult
    package mutating func resume() -> Int? {
        guard dataset.phase == .cancelled || dataset.phase == .offline else { return nil }
        let offset = dataset.currentAttempt?.downloadedBytes ?? 0
        return beginAttempt(kind: .resume, resumeOffsetBytes: offset)
    }

    @discardableResult
    package mutating func restart() -> Int? {
        guard dataset.phase != .missing,
              dataset.phase != .approvalRequired,
              dataset.phase != .ready,
              dataset.phase != .diskFailed
        else { return nil }
        if dataset.phase.isRunning {
            transition(to: .cancelled, message: "Superseded by a fresh acquisition generation.")
        }
        return beginAttempt(kind: .restart, resumeOffsetBytes: 0)
    }

    @discardableResult
    package mutating func retry() -> Int? {
        let offset: UInt64
        switch dataset.phase {
        case .offline:
            offset = dataset.currentAttempt?.downloadedBytes ?? 0
        case .checksumFailed, .unsafeArchive:
            offset = 0
        case .missing, .approvalRequired, .downloading, .verifying, .unpacking,
             .ready, .cancelled, .diskFailed:
            return nil
        }
        return beginAttempt(kind: .retry, resumeOffsetBytes: offset)
    }

    @discardableResult
    package mutating func makeSpaceAndRetry() -> Int? {
        guard dataset.phase == .diskFailed else { return nil }
        approvalFacts.freeDiskBytes = max(approvalFacts.freeDiskBytes, 4_000_000_000)
        return beginAttempt(kind: .makeSpaceRetry, resumeOffsetBytes: 0)
    }

    @discardableResult
    package mutating func selectSection(id: String) -> Bool {
        guard let index = sections.firstIndex(where: { $0.id == id }),
              sections[index].status != .blocked
        else { return false }
        selectedSectionID = id
        if sections[index].status == .notStarted {
            sections[index].status = .inProgress
        }
        return true
    }

    private mutating func beginAttempt(
        kind: TutorialNotebookAttemptKind,
        resumeOffsetBytes: UInt64
    ) -> Int {
        let generation = dataset.currentGeneration + 1
        dataset.currentGeneration = generation
        dataset.attempts.append(TutorialNotebookDatasetAttempt(
            generation: generation,
            kind: kind,
            phase: .downloading,
            resumeOffsetBytes: resumeOffsetBytes,
            downloadedBytes: resumeOffsetBytes,
            expectedBytes: approvalFacts.expectedSizeBytes,
            message: nil
        ))
        transition(to: .downloading, downloadedBytes: resumeOffsetBytes)
        return generation
    }

    private mutating func transition(
        to phase: TutorialNotebookAcquisitionPhase,
        downloadedBytes: UInt64? = nil,
        message: String? = nil
    ) {
        dataset.phase = phase
        dataset.isStaged = phase == .ready
        dataset.message = message
        guard let index = dataset.attempts.lastIndex(where: {
            $0.generation == dataset.currentGeneration
        }) else { return }
        dataset.attempts[index].phase = phase
        if let downloadedBytes {
            dataset.attempts[index].downloadedBytes = downloadedBytes
        }
        dataset.attempts[index].message = message
    }

    private mutating func unlockDatasetSections() {
        for index in sections.indices {
            if sections[index].id == "tutorial-section-acquisition" {
                sections[index].status = .completed
            } else if sections[index].status == .blocked {
                sections[index].status = .notStarted
            }
        }
    }
}

/// Deterministic Wave 3 fixture adapter. It performs no file, network, archive,
/// disk, provider, task, or notebook-persistence operation.
package enum TutorialNotebookPrototypeFixtureAdapter {
    package static let expectedSizeBytes: UInt64 = 435_742_720
    package static let expectedSHA256 =
        "f0cfeee5b9dec09ac9ed4d3e4e048d5eb28023c11cbc8295c09ddefe6b8a97b2"

    package static func make(
        scenario: TutorialNotebookPrototypeScenario
    ) -> TutorialNotebookPrototypeProjection {
        let task = fixtureTask()
        let notebook = learnerNotebook(task: task)
        let requiredDiskBytes = expectedSizeBytes * 3
        let freeDiskBytes: UInt64 = scenario == .diskFailure ? 512_000_000 : 4_000_000_000
        let source = "https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.1/twhya_calibrated.ms.tar"
        let approval = TutorialNotebookApprovalFacts(
            scheme: "https",
            requestedURL: source,
            resolvedURL: source,
            redirects: [],
            expectedSizeBytes: expectedSizeBytes,
            destination: "data/twhya_calibrated.ms",
            expectedSHA256: expectedSHA256,
            requiredDiskBytes: requiredDiskBytes,
            freeDiskBytes: freeDiskBytes,
            extractionPlan: "Verify the tar archive, then safely materialize twhya_calibrated.ms under project data; reject traversal, escaping links, device files, archive bombs, and collisions.",
            optionalChecks: [
                TutorialNotebookOptionalCheck(
                    id: "check-ms-structure",
                    label: "Check MeasurementSet main and required subtables",
                    isEnabled: true
                ),
                TutorialNotebookOptionalCheck(
                    id: "check-twhya-field",
                    label: "Confirm the TW Hya target field is present",
                    isEnabled: true
                ),
            ]
        )

        return TutorialNotebookPrototypeProjection(
            prototypeKind: .tutorial,
            scenario: scenario,
            title: "First Look at Imaging: TW Hya",
            sections: [
                TutorialNotebookSectionProjection(
                    id: "tutorial-section-welcome",
                    title: "Welcome",
                    status: .completed
                ),
                TutorialNotebookSectionProjection(
                    id: "tutorial-section-acquisition",
                    title: "Acquire the calibrated data",
                    status: .inProgress
                ),
                TutorialNotebookSectionProjection(
                    id: "tutorial-section-inspect",
                    title: "Inspect the observation",
                    status: .blocked
                ),
                TutorialNotebookSectionProjection(
                    id: "tutorial-section-image",
                    title: "Load imaging parameters",
                    status: .blocked
                ),
            ],
            selectedSectionID: "tutorial-section-acquisition",
            dataset: TutorialNotebookDatasetProjection(
                id: "tutorial-dataset-twhya-calibrated",
                name: "TW Hya calibrated MeasurementSet",
                archiveName: "twhya_calibrated.ms.tar",
                destination: approval.destination,
                phase: .missing,
                isStaged: false,
                currentGeneration: 0,
                attempts: [],
                message: "Dataset is missing. Review acquisition details to continue."
            ),
            approvalFacts: approval,
            activeApproval: nil,
            learnerNotebook: notebook,
            fixtureTask: task,
            scenarioFailureConsumed: false
        )
    }

    private static func fixtureTask() -> PrototypeNotebookTaskProjection {
        PrototypeNotebookTaskProjection(
            id: "tutorial-task-twhya-imager",
            taskID: "imager",
            title: "Create a TW Hya continuum image",
            annotation: "Load the tutorial parameters without running the task.",
            contractVersion: 1,
            sparseProfileTOML: """
            [casars]
            format = 1
            surface = "imager"
            kind = "task"
            contract = 1

            [parameters]
            vis = "data/twhya_calibrated.ms"
            imagename = "products/twhya-continuum"
            field = "5"
            specmode = "mfs"
            imsize = 250
            cell = "0.1arcsec"
            weighting = "briggs"
            robust = 0.5
            """,
            parameterRows: [
                PrototypeNotebookParameterRow(parameterID: "vis", label: "MeasurementSet", value: "data/twhya_calibrated.ms"),
                PrototypeNotebookParameterRow(parameterID: "imagename", label: "Image name", value: "products/twhya-continuum"),
                PrototypeNotebookParameterRow(parameterID: "field", label: "Field", value: "5"),
                PrototypeNotebookParameterRow(parameterID: "specmode", label: "Spectral mode", value: "mfs"),
                PrototypeNotebookParameterRow(parameterID: "imsize", label: "Image size", value: "250"),
                PrototypeNotebookParameterRow(parameterID: "cell", label: "Cell size", value: "0.1arcsec"),
                PrototypeNotebookParameterRow(parameterID: "weighting", label: "Weighting", value: "briggs"),
                PrototypeNotebookParameterRow(parameterID: "robust", label: "Robust", value: "0.5"),
            ],
            revisions: []
        )
    }

    private static func learnerNotebook(
        task: PrototypeNotebookTaskProjection
    ) -> PrototypeScientificNotebookProjection {
        let markdown = """
        # First Look at Imaging: TW Hya

        This learner copy is editable. The tutorial template remains immutable.

        ## Acquire the calibrated data

        My goal: understand the calibrated observation before imaging.

        ## Load imaging parameters

        Review these sparse parameters before choosing whether to run the task.

        \(taskCell(id: task.id, profileTOML: task.sparseProfileTOML))
        """
        let document = PrototypeNotebookDocumentProjection(
            id: "tutorial-notebook-twhya-first-look",
            title: "First Look at Imaging: TW Hya",
            filename: "TW Hya First Look.md",
            displayPath: "notebooks/TW Hya First Look.md",
            savedMarkdown: markdown,
            draftMarkdown: markdown,
            hasExternalConflict: false,
            tasks: [task],
            selectedReceiptID: nil
        )
        return PrototypeScientificNotebookProjection(
            scenario: .primary,
            documents: [document],
            activeNotebookID: document.id
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
}
