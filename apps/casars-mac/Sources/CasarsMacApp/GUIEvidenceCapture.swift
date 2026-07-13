import CasarsMacCore
import AppKit
import Foundation
import SwiftUI

extension CasarsMacApp {
    @MainActor
    static func captureGUIEvidence(arguments: [String]) {
        do {
            let request = try GUIEvidenceCaptureRequest(arguments: arguments)
            try GUIEvidenceCaptureRenderer.render(request: request)
        } catch {
            fputs("GUI evidence capture failed: \(error)\n", stderr)
            exit(1)
        }
    }
}

private struct GUIEvidenceCaptureRequest {
    enum CaptureKind: String {
        case measurementSetPlot = "measurement-set-plot"
        case measurementSetSummary = "measurement-set-summary"
        case splitRun = "split-run"
        case imagerParameters = "imager-parameters"
        case imagerRun = "imager-run"
        case imagerProgressMockup = "imager-progress-mockup"
        case impbcorParameters = "impbcor-parameters"
        case impbcorRun = "impbcor-run"
        case imageExplorer = "image-explorer"
        case notebookPrototype = "notebook-prototype"
        case pythonPrototype = "python-prototype"
        case tutorialPrototype = "tutorial-prototype"
        case aiPrototype = "ai-prototype"

        var requiresTutorialPack: Bool {
            switch self {
            case .imagerProgressMockup, .notebookPrototype, .pythonPrototype, .tutorialPrototype, .aiPrototype:
                false
            default:
                true
            }
        }
    }

    var captureKind: CaptureKind
    var tutorialPackPath: String?
    var datasetName: String
    var imagePath: String?
    var pbImagePath: String?
    var outfilePath: String?
    var outputPrefix: String?
    var field: String
    var dirtyOnly: Bool
    var writePB: Bool
    var pbLimit: String
    var niter: String
    var thresholdJy: String
    var cutoff: String
    var maskBox: String?
    var preset: MeasurementSetExplorerPlotPreset
    var iterationAxis: MeasurementSetPlotIterationAxis?
    var prototypeScenario: NotebookPrototypeScenario
    var pythonPrototypeScenario: PythonPrototypeScenario
    var tutorialPrototypeScenario: TutorialNotebookPrototypeScenario
    var aiPrototypeScenario: AIChatPrototypeScenario
    var outputPath: String
    var width: CGFloat
    var height: CGFloat

    init(arguments: [String]) throws {
        let captureKind = CaptureKind(rawValue: Self.argumentValue(after: "--capture-kind", in: arguments) ?? CaptureKind.measurementSetPlot.rawValue) ?? .measurementSetPlot
        if let error = CasarsMacApp.prototypeLaunchValidationError(arguments: arguments) {
            throw GUIEvidenceCaptureError.invalidArgument(error)
        }
        let tutorialPackPath = Self.argumentValue(after: "--open-tutorial-pack", in: arguments)
        if captureKind.requiresTutorialPack && tutorialPackPath == nil {
            throw GUIEvidenceCaptureError.missingArgument("--open-tutorial-pack")
        }
        guard let outputPath = Self.argumentValue(after: "--output", in: arguments) else {
            throw GUIEvidenceCaptureError.missingArgument("--output")
        }
        self.captureKind = captureKind
        self.tutorialPackPath = tutorialPackPath
        self.datasetName = Self.argumentValue(after: "--dataset", in: arguments) ?? "twhya_calibrated.ms"
        self.imagePath = Self.argumentValue(after: "--image", in: arguments)
        self.pbImagePath = Self.argumentValue(after: "--pbimage", in: arguments)
        self.outfilePath = Self.argumentValue(after: "--outfile", in: arguments)
        self.outputPrefix = Self.argumentValue(after: "--imagename", in: arguments)
        self.field = Self.argumentValue(after: "--field", in: arguments) ?? "3"
        self.dirtyOnly = Self.argumentValue(after: "--dirty-only", in: arguments)
            .map(Self.parseBool) ?? false
        self.writePB = Self.argumentValue(after: "--write-pb", in: arguments)
            .map(Self.parseBool) ?? false
        self.pbLimit = Self.argumentValue(after: "--pblimit", in: arguments) ?? "0.2"
        self.niter = Self.argumentValue(after: "--niter", in: arguments) ?? "0"
        self.thresholdJy = Self.argumentValue(after: "--threshold-jy", in: arguments) ?? "0.0"
        self.cutoff = Self.argumentValue(after: "--cutoff", in: arguments) ?? "-1.0"
        self.maskBox = Self.argumentValue(after: "--mask-box", in: arguments)
        self.preset = Self.plotPreset(Self.argumentValue(after: "--preset", in: arguments)) ?? .uvCoverage
        self.iterationAxis = Self.iterationAxis(Self.argumentValue(after: "--iteraxis", in: arguments))
        self.prototypeScenario = try Self.prototypeScenario(Self.argumentValue(after: "--prototype-state", in: arguments))
        self.pythonPrototypeScenario = try Self.pythonPrototypeScenario(
            Self.argumentValue(after: "--prototype-state", in: arguments)
        )
        self.tutorialPrototypeScenario = try Self.tutorialPrototypeScenario(
            Self.argumentValue(after: "--prototype-state", in: arguments)
        )
        self.aiPrototypeScenario = Self.aiPrototypeScenario(
            Self.argumentValue(after: "--prototype-state", in: arguments)
        )
        self.outputPath = outputPath
        self.width = CGFloat(Double(Self.argumentValue(after: "--width", in: arguments) ?? "1440") ?? 1440)
        self.height = CGFloat(Double(Self.argumentValue(after: "--height", in: arguments) ?? "960") ?? 960)
    }

    private static func argumentValue(after flag: String, in arguments: [String]) -> String? {
        guard let index = arguments.firstIndex(of: flag) else { return nil }
        let valueIndex = arguments.index(after: index)
        guard valueIndex < arguments.endIndex else { return nil }
        return arguments[valueIndex]
    }

    private static func parseBool(_ value: String) -> Bool {
        switch value.lowercased() {
        case "1", "true", "yes", "on":
            true
        default:
            false
        }
    }

    private static func plotPreset(_ value: String?) -> MeasurementSetExplorerPlotPreset? {
        switch value?.lowercased() {
        case nil, "", "uv_coverage", "uvcoverage":
            .uvCoverage
        case "amplitude_vs_uv_distance", "amplitude_vs_uvdist", "amp_uvdist":
            .amplitudeVsUvDistance
        default:
            nil
        }
    }

    private static func iterationAxis(_ value: String?) -> MeasurementSetPlotIterationAxis? {
        switch value?.lowercased() {
        case "field":
            .field
        case "scan":
            .scan
        case "spw", "spectral_window", "spectralwindow":
            .spectralWindow
        case "correlation":
            .correlation
        default:
            nil
        }
    }

    private static func prototypeScenario(_ value: String?) throws -> NotebookPrototypeScenario {
        switch value {
        case "external-conflict":
            .externalConflict
        default:
            .primary
        }
    }

    private static func pythonPrototypeScenario(_ value: String?) throws -> PythonPrototypeScenario {
        switch value {
        case "failure": .failure
        case "nonresponsive": .nonresponsive
        default: .primary
        }
    }

    private static func tutorialPrototypeScenario(
        _ value: String?
    ) throws -> TutorialNotebookPrototypeScenario {
        switch value {
        case "checksum-failure": .checksumFailure
        case "disk-failure": .diskFailure
        case "offline": .offline
        case "unsafe-archive": .unsafeArchive
        default: .happyPath
        }
    }

    private static func aiPrototypeScenario(_ value: String?) -> AIChatPrototypeScenario {
        switch value {
        case "provider-error": .providerError
        case "rate-limited": .rateLimited
        case "offline": .offline
        case "tool-failure": .toolFailure
        case "nonresponsive": .nonresponsive
        default: .primary
        }
    }
}

private enum GUIEvidenceCaptureError: Error, CustomStringConvertible {
    case invalidArgument(String)
    case missingArgument(String)
    case datasetNotFound(String)
    case imageNotProvided
    case plotFailed(String)
    case taskFailed(String)
    case imageEncodingFailed

    var description: String {
        switch self {
        case .invalidArgument(let message):
            message
        case .missingArgument(let argument):
            "missing required argument \(argument)"
        case .datasetNotFound(let name):
            "could not find MeasurementSet dataset \(name)"
        case .imageNotProvided:
            "missing required --image path for image-explorer capture"
        case .plotFailed(let message):
            "MeasurementSet plot did not finish: \(message)"
        case .taskFailed(let message):
            "task did not finish: \(message)"
        case .imageEncodingFailed:
            "could not encode PNG image"
        }
    }
}

private enum GUIEvidenceCaptureRenderer {
    @MainActor
    static func render(request: GUIEvidenceCaptureRequest) throws {
        NSApplication.shared.setActivationPolicy(.accessory)

        let store: WorkbenchStore = switch request.captureKind {
        case .notebookPrototype:
            WorkbenchStore.notebookPrototype(scenario: request.prototypeScenario)
        case .pythonPrototype:
            WorkbenchStore.pythonPrototype(scenario: request.pythonPrototypeScenario)
        case .tutorialPrototype:
            WorkbenchStore.tutorialPrototype(scenario: request.tutorialPrototypeScenario)
        case .aiPrototype:
            WorkbenchStore.aiPrototype(scenario: request.aiPrototypeScenario)
        default:
            WorkbenchStore.empty()
        }
        store.setInterfaceFontSize(WorkbenchState.defaultInterfaceFontSize)
        if let tutorialPackPath = request.tutorialPackPath {
            store.openTutorialTemplate(path: tutorialPackPath)
        }

        switch request.captureKind {
        case .measurementSetPlot:
            try renderMeasurementSetPlot(request: request, store: store)
        case .measurementSetSummary:
            try renderMeasurementSetSummary(request: request, store: store)
        case .splitRun:
            try renderSplitRun(request: request, store: store)
        case .imagerParameters:
            try renderImagerParameters(request: request, store: store)
        case .imagerRun:
            try renderImagerRun(request: request, store: store)
        case .imagerProgressMockup:
            try renderImagerProgressMockup(request: request, store: store)
        case .impbcorParameters:
            try renderImpbcorParameters(request: request, store: store)
        case .impbcorRun:
            try renderImpbcorRun(request: request, store: store)
        case .imageExplorer:
            try renderImageExplorer(request: request, store: store)
        case .notebookPrototype:
            try renderNotebookPrototype(request: request, store: store)
        case .pythonPrototype:
            try renderPythonPrototype(request: request, store: store)
        case .tutorialPrototype:
            try renderTutorialPrototype(request: request, store: store)
        case .aiPrototype:
            try renderAIPrototype(request: request, store: store)
        }
    }

    @MainActor
    private static func renderNotebookPrototype(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func renderPythonPrototype(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func renderTutorialPrototype(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func renderAIPrototype(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        store.openAIPrototypeDrawer()
        store.sendAIPrototypePrompt("Compare the current plot with the TW Hya paper.")
        let responseDeadline = Date().addingTimeInterval(2.0)
        while Date() < responseDeadline,
              store.state.prototypeAI?.messages.contains(where: { $0.role == .assistant }) != true
        {
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.05))
        }
        store.showAIPrototypeNotebookSuggestions()
        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func renderMeasurementSetSummary(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        guard let dataset = store.state.project.datasets.first(where: { dataset in
            dataset.kind == .measurementSet
                && (dataset.name == request.datasetName || URL(fileURLWithPath: dataset.path).lastPathComponent == request.datasetName)
        }) else {
            throw GUIEvidenceCaptureError.datasetNotFound(request.datasetName)
        }

        store.selectDataset(dataset.id)
        store.openSelectedDatasetExplorer()

        let view = WorkbenchView(
            store: store,
            initialMeasurementSetExplorerMode: .summary
        )
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)

        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func renderMeasurementSetPlot(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        guard let dataset = store.state.project.datasets.first(where: { dataset in
            dataset.kind == .measurementSet
                && (dataset.name == request.datasetName || URL(fileURLWithPath: dataset.path).lastPathComponent == request.datasetName)
        }) else {
            throw GUIEvidenceCaptureError.datasetNotFound(request.datasetName)
        }

        store.selectDataset(dataset.id)
        store.setMeasurementSetPlotPreset(request.preset, datasetID: dataset.id)
        store.setMeasurementSetPlotAvgChannel(10_000, datasetID: dataset.id)
        store.setMeasurementSetPlotAvgTime(1.0e9, datasetID: dataset.id)
        store.setMeasurementSetPlotAvgSPW(false, datasetID: dataset.id)
        store.setMeasurementSetPlotAvgScan(false, datasetID: dataset.id)
        store.setMeasurementSetPlotIterationAxis(request.iterationAxis, datasetID: dataset.id)
        store.runMeasurementSetPlot(datasetID: dataset.id)

        guard waitForPlot(store: store, datasetID: dataset.id, timeoutSeconds: 120) else {
            let error = store.state.measurementSetPlots[dataset.id]?.lastError ?? "timed out"
            throw GUIEvidenceCaptureError.plotFailed(error)
        }

        store.openSelectedDatasetExplorer()

        let view = WorkbenchView(
            store: store,
            initialMeasurementSetExplorerMode: .plots
        )
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)

        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )

        let outputURL = URL(fileURLWithPath: request.outputPath).standardizedFileURL
        try FileManager.default.createDirectory(
            at: outputURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try png.write(to: outputURL)
        print("Wrote GUI evidence screenshot \(outputURL.path)")
    }

    @MainActor
    private static func renderSplitRun(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        guard let dataset = store.state.project.datasets.first(where: { dataset in
            dataset.kind == .measurementSet
                && (dataset.name == request.datasetName || URL(fileURLWithPath: dataset.path).lastPathComponent == request.datasetName)
        }) else {
            throw GUIEvidenceCaptureError.datasetNotFound(request.datasetName)
        }

        store.selectDataset(dataset.id)
        store.selectTask("split")
        store.setGenericTaskValue(taskID: "split", argumentID: "vis", value: dataset.path)
        store.setGenericTaskValue(taskID: "split", argumentID: "outputvis", value: "twhya_smoothed.gui.ms")
        store.setGenericTaskValue(taskID: "split", argumentID: "field", value: "5")
        store.setGenericTaskValue(taskID: "split", argumentID: "spw", value: "0")
        store.setGenericTaskValue(taskID: "split", argumentID: "scan", value: "")
        store.setGenericTaskValue(taskID: "split", argumentID: "antenna", value: "")
        store.setGenericTaskValue(taskID: "split", argumentID: "timerange", value: "")
        store.setGenericTaskValue(taskID: "split", argumentID: "msselect", value: "")
        store.setGenericTaskValue(taskID: "split", argumentID: "width", value: "8")
        store.setGenericTaskValue(taskID: "split", argumentID: "datacolumn", value: "DATA")
        store.setGenericTaskToggle(taskID: "split", argumentID: "keepflags", value: true)
        store.setGenericTaskConfirmation(taskID: "split", confirmed: true)
        store.runTask()
        guard waitForTask(store: store, timeoutSeconds: 180) else {
            throw GUIEvidenceCaptureError.taskFailed(
                store.state.taskRun.diagnostics.joined(separator: "\n").isEmpty
                    ? "timed out"
                    : store.state.taskRun.diagnostics.joined(separator: "\n")
            )
        }
        guard store.state.taskRun.state == .succeeded else {
            throw GUIEvidenceCaptureError.taskFailed(
                store.state.taskRun.diagnostics.joined(separator: "\n").isEmpty
                    ? store.state.taskRun.logLines.joined(separator: "\n")
                    : store.state.taskRun.diagnostics.joined(separator: "\n")
            )
        }

        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func renderImagerParameters(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        guard let dataset = store.state.project.datasets.first(where: { dataset in
            dataset.kind == .measurementSet
                && (dataset.name == request.datasetName || URL(fileURLWithPath: dataset.path).lastPathComponent == request.datasetName)
        }) else {
            throw GUIEvidenceCaptureError.datasetNotFound(request.datasetName)
        }

        store.selectDataset(dataset.id)
        store.openImagerTaskForSelectedDataset()
        store.selectTask("imager")
        store.setGenericTaskValue(taskID: "imager", argumentID: "vis", value: dataset.path)
        store.setGenericTaskValue(taskID: "imager", argumentID: "imagename", value: request.outputPrefix ?? "phase_cal")
        store.setGenericTaskValue(taskID: "imager", argumentID: "field", value: request.field)
        store.setGenericTaskValue(taskID: "imager", argumentID: "phasecenter_field", value: request.field)
        store.setGenericTaskValue(taskID: "imager", argumentID: "imsize", value: "250")
        store.setGenericTaskValue(taskID: "imager", argumentID: "cell", value: "0.1arcsec")
        store.setGenericTaskValue(taskID: "imager", argumentID: "weighting", value: "briggs")
        store.setGenericTaskValue(taskID: "imager", argumentID: "gridder", value: "standard")
        store.setGenericTaskValue(taskID: "imager", argumentID: "robust", value: "0.5")
        store.setGenericTaskValue(taskID: "imager", argumentID: "niter", value: request.niter)
        store.setGenericTaskValue(taskID: "imager", argumentID: "threshold", value: "\(request.thresholdJy)Jy")
        store.setGenericTaskValue(taskID: "imager", argumentID: "pblimit", value: request.pbLimit)
        store.setGenericTaskToggle(taskID: "imager", argumentID: "dirty_only", value: request.dirtyOnly)
        store.setGenericTaskToggle(taskID: "imager", argumentID: "write_pb", value: request.writePB)
        if let maskBox = request.maskBox {
            store.setGenericTaskValue(taskID: "imager", argumentID: "mask_box", value: maskBox)
        }

        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func renderImagerRun(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        try configureImager(request: request, store: store)
        store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
        store.runTask()
        guard waitForTask(store: store, timeoutSeconds: 300) else {
            throw GUIEvidenceCaptureError.taskFailed(
                store.state.taskRun.diagnostics.joined(separator: "\n").isEmpty
                    ? "timed out"
                    : store.state.taskRun.diagnostics.joined(separator: "\n")
            )
        }
        guard store.state.taskRun.state == .succeeded else {
            throw GUIEvidenceCaptureError.taskFailed(
                store.state.taskRun.diagnostics.joined(separator: "\n").isEmpty
                    ? store.state.taskRun.logLines.joined(separator: "\n")
                    : store.state.taskRun.diagnostics.joined(separator: "\n")
            )
        }

        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func renderImagerProgressMockup(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        store.openImagerProgressMockup()

        guard store.state.taskRun.imagerProgress != nil else {
            throw GUIEvidenceCaptureError.taskFailed("imager progress mockup did not populate progress telemetry")
        }

        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func renderImpbcorParameters(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        try configureImpbcor(request: request, store: store)

        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func renderImpbcorRun(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        try configureImpbcor(request: request, store: store)
        store.setGenericTaskConfirmation(taskID: "impbcor", confirmed: true)
        store.runTask()
        guard waitForTask(store: store, timeoutSeconds: 180) else {
            throw GUIEvidenceCaptureError.taskFailed(
                store.state.taskRun.diagnostics.joined(separator: "\n").isEmpty
                    ? "timed out"
                    : store.state.taskRun.diagnostics.joined(separator: "\n")
            )
        }
        guard store.state.taskRun.state == .succeeded else {
            throw GUIEvidenceCaptureError.taskFailed(
                store.state.taskRun.diagnostics.joined(separator: "\n").isEmpty
                    ? store.state.taskRun.logLines.joined(separator: "\n")
                    : store.state.taskRun.diagnostics.joined(separator: "\n")
            )
        }

        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    @MainActor
    private static func configureImpbcor(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        guard let imagePath = request.imagePath else {
            throw GUIEvidenceCaptureError.imageNotProvided
        }
        guard let pbImagePath = request.pbImagePath else {
            throw GUIEvidenceCaptureError.missingArgument("--pbimage")
        }
        guard let outfilePath = request.outfilePath else {
            throw GUIEvidenceCaptureError.missingArgument("--outfile")
        }

        store.selectTask("impbcor")
        store.openTab(
            WorkbenchTab(
                id: "tab-gui-evidence-impbcor",
                title: "Primary Beam Correction",
                kind: .task,
                datasetID: store.state.selectedDatasetID,
                taskID: "impbcor"
            )
        )
        store.setGenericTaskValue(taskID: "impbcor", argumentID: "imagename", value: imagePath)
        store.setGenericTaskValue(taskID: "impbcor", argumentID: "pbimage", value: pbImagePath)
        store.setGenericTaskValue(taskID: "impbcor", argumentID: "outfile", value: outfilePath)
        store.setGenericTaskValue(taskID: "impbcor", argumentID: "cutoff", value: request.cutoff)
        store.setGenericTaskValue(taskID: "impbcor", argumentID: "mode", value: "divide")
        store.setGenericTaskToggle(taskID: "impbcor", argumentID: "overwrite", value: true)
    }

    @MainActor
    private static func configureImager(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        guard let dataset = store.state.project.datasets.first(where: { dataset in
            dataset.kind == .measurementSet
                && (dataset.name == request.datasetName || URL(fileURLWithPath: dataset.path).lastPathComponent == request.datasetName)
        }) else {
            throw GUIEvidenceCaptureError.datasetNotFound(request.datasetName)
        }

        store.selectDataset(dataset.id)
        store.openImagerTaskForSelectedDataset()
        store.selectTask("imager")
        store.setGenericTaskValue(taskID: "imager", argumentID: "vis", value: dataset.path)
        store.setGenericTaskValue(taskID: "imager", argumentID: "imagename", value: request.outputPrefix ?? "phase_cal")
        store.setGenericTaskValue(taskID: "imager", argumentID: "field", value: request.field)
        store.setGenericTaskValue(taskID: "imager", argumentID: "phasecenter_field", value: request.field)
        store.setGenericTaskValue(taskID: "imager", argumentID: "imsize", value: "250")
        store.setGenericTaskValue(taskID: "imager", argumentID: "cell", value: "0.1arcsec")
        store.setGenericTaskValue(taskID: "imager", argumentID: "weighting", value: "briggs")
        store.setGenericTaskValue(taskID: "imager", argumentID: "gridder", value: "standard")
        store.setGenericTaskValue(taskID: "imager", argumentID: "robust", value: "0.5")
        store.setGenericTaskValue(taskID: "imager", argumentID: "niter", value: request.niter)
        store.setGenericTaskValue(taskID: "imager", argumentID: "threshold", value: "\(request.thresholdJy)Jy")
        store.setGenericTaskValue(taskID: "imager", argumentID: "pblimit", value: request.pbLimit)
        store.setGenericTaskToggle(taskID: "imager", argumentID: "dirty_only", value: request.dirtyOnly)
        store.setGenericTaskToggle(taskID: "imager", argumentID: "write_pb", value: request.writePB)
        if let maskBox = request.maskBox {
            store.setGenericTaskValue(taskID: "imager", argumentID: "mask_box", value: maskBox)
        }
    }

    @MainActor
    private static func renderImageExplorer(
        request: GUIEvidenceCaptureRequest,
        store: WorkbenchStore
    ) throws {
        guard let imagePath = request.imagePath else {
            throw GUIEvidenceCaptureError.imageNotProvided
        }
        let sourceDataset = store.state.project.datasets.first(where: { dataset in
            dataset.kind == .measurementSet
                && (dataset.name == request.datasetName || URL(fileURLWithPath: dataset.path).lastPathComponent == request.datasetName)
        })
        store.openImageExplorerPath(imagePath, sourceDatasetID: sourceDataset?.id)

        let view = WorkbenchView(store: store)
            .environment(\.workbenchFontSize, WorkbenchState.defaultInterfaceFontSize)
            .preferredColorScheme(.dark)
            .frame(width: request.width, height: request.height)
        let png = try renderPNGWithHostingView(
            view: view,
            width: request.width,
            height: request.height,
            scale: 2.0
        )
        try writePNG(png, outputPath: request.outputPath)
    }

    private static func renderPNGWithHostingView<Content: View>(
        view: Content,
        width: CGFloat,
        height: CGFloat,
        scale: CGFloat
    ) throws -> Data {
        let frame = NSRect(x: 0, y: 0, width: width, height: height)
        let hostingView = NSHostingView(rootView: view)
        hostingView.frame = frame
        hostingView.appearance = NSAppearance(named: .darkAqua)

        let window = NSWindow(
            contentRect: frame,
            styleMask: [.borderless],
            backing: .buffered,
            defer: false
        )
        window.contentView = hostingView
        window.isReleasedWhenClosed = false
        window.backgroundColor = .windowBackgroundColor
        window.layoutIfNeeded()
        hostingView.layoutSubtreeIfNeeded()

        for _ in 0..<6 {
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.05))
            window.displayIfNeeded()
            hostingView.layoutSubtreeIfNeeded()
        }

        guard let representation = NSBitmapImageRep(
            bitmapDataPlanes: nil,
            pixelsWide: Int(width * scale),
            pixelsHigh: Int(height * scale),
            bitsPerSample: 8,
            samplesPerPixel: 4,
            hasAlpha: true,
            isPlanar: false,
            colorSpaceName: .deviceRGB,
            bytesPerRow: 0,
            bitsPerPixel: 0
        ) else {
            throw GUIEvidenceCaptureError.imageEncodingFailed
        }
        representation.size = NSSize(width: width, height: height)
        hostingView.cacheDisplay(in: hostingView.bounds, to: representation)
        window.close()

        guard let png = representation.representation(using: .png, properties: [:]) else {
            throw GUIEvidenceCaptureError.imageEncodingFailed
        }
        return png
    }

    private static func writePNG(_ png: Data, outputPath: String) throws {
        let outputURL = URL(fileURLWithPath: outputPath).standardizedFileURL
        try FileManager.default.createDirectory(
            at: outputURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try png.write(to: outputURL)
        print("Wrote GUI evidence screenshot \(outputURL.path)")
    }

    private static func waitForPlot(
        store: WorkbenchStore,
        datasetID: String,
        timeoutSeconds: TimeInterval
    ) -> Bool {
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while Date() < deadline {
            let plotState = store.state.measurementSetPlots[datasetID]
            if plotState?.status == .ready && plotState?.result != nil {
                return true
            }
            if plotState?.status == .failed {
                return false
            }
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.05))
        }
        return false
    }

    private static func waitForTask(
        store: WorkbenchStore,
        timeoutSeconds: TimeInterval
    ) -> Bool {
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while Date() < deadline {
            if store.state.taskRun.state == .succeeded || store.state.taskRun.state == .failed {
                return true
            }
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.05))
        }
        return false
    }
}
