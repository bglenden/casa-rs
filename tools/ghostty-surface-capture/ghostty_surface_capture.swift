import AppKit
import CoreImage
import Foundation
import GhosttyKit
import ImageIO
import IOSurface
import UniformTypeIdentifiers

struct CaptureOptions {
    var output: String?
    var cwd: String = FileManager.default.currentDirectoryPath
    var width: CGFloat = 1600
    var height: CGFloat = 1000
    var fontSize: Float = 14
    var settleSeconds: TimeInterval = 6
    var initialInput: String?
    var inputEvents: [(TimeInterval, String)] = []
    var command: [String] = []
}

enum CaptureError: Error, CustomStringConvertible {
    case usage(String)
    case ghosttyInit(Int32)
    case configCreate
    case appCreate
    case surfaceCreate
    case missingLayer
    case missingLayerContents
    case unsupportedLayerContents(Any)
    case imageCreate
    case imageWrite(URL)

    var description: String {
        switch self {
        case .usage(let message): return message
        case .ghosttyInit(let code): return "ghostty_init failed with code \(code)"
        case .configCreate: return "ghostty_config_new failed"
        case .appCreate: return "ghostty_app_new failed"
        case .surfaceCreate: return "ghostty_surface_new failed"
        case .missingLayer: return "Ghostty did not attach a layer to the capture view"
        case .missingLayerContents: return "Ghostty's renderer layer did not publish IOSurface or CGImage contents"
        case .unsupportedLayerContents(let value):
            return "unsupported Ghostty layer contents type: \(type(of: value))"
        case .imageCreate: return "failed to create CGImage from Ghostty layer contents"
        case .imageWrite(let url): return "failed to write PNG to \(url.path)"
        }
    }
}

final class CaptureView: NSView {
    override var acceptsFirstResponder: Bool { true }
}

func parseOptions() throws -> CaptureOptions {
    var options = CaptureOptions()
    var iterator = CommandLine.arguments.dropFirst().makeIterator()

    while let arg = iterator.next() {
        switch arg {
        case "--output":
            guard let value = iterator.next() else { throw CaptureError.usage("--output requires a path") }
            options.output = value
        case "--cwd":
            guard let value = iterator.next() else { throw CaptureError.usage("--cwd requires a directory") }
            options.cwd = value
        case "--width":
            guard let value = iterator.next(), let parsed = Double(value) else {
                throw CaptureError.usage("--width requires a number")
            }
            options.width = CGFloat(parsed)
        case "--height":
            guard let value = iterator.next(), let parsed = Double(value) else {
                throw CaptureError.usage("--height requires a number")
            }
            options.height = CGFloat(parsed)
        case "--font-size":
            guard let value = iterator.next(), let parsed = Float(value) else {
                throw CaptureError.usage("--font-size requires a number")
            }
            options.fontSize = parsed
        case "--settle-seconds":
            guard let value = iterator.next(), let parsed = Double(value) else {
                throw CaptureError.usage("--settle-seconds requires a number")
            }
            options.settleSeconds = parsed
        case "--input":
            guard let value = iterator.next() else { throw CaptureError.usage("--input requires text") }
            options.initialInput = value
        case "--input-event":
            guard let value = iterator.next() else {
                throw CaptureError.usage("--input-event requires MS:TEXT")
            }
            let parts = value.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false)
            guard parts.count == 2, let milliseconds = Double(parts[0]) else {
                throw CaptureError.usage("--input-event requires MS:TEXT")
            }
            options.inputEvents.append((milliseconds / 1000.0, String(parts[1])))
        case "--":
            options.command = Array(iterator)
            if options.command.isEmpty {
                throw CaptureError.usage("missing command after --")
            }
            return options
        default:
            throw CaptureError.usage("unknown argument: \(arg)")
        }
    }

    throw CaptureError.usage("usage: ghostty-surface-capture --output PATH [--cwd DIR] [--width N] [--height N] [--font-size N] [--settle-seconds N] [--input TEXT] [--input-event MS:TEXT] -- COMMAND [ARGS...]")
}

func sendText(_ text: String, to surface: ghostty_surface_t) {
    text.withCString { pointer in
        ghostty_surface_text(surface, pointer, UInt(text.utf8.count))
    }
}

func runMainLoop(
    until deadline: Date,
    app: ghostty_app_t,
    surface: ghostty_surface_t,
    inputEvents: [(TimeInterval, String)]
) {
    let start = Date()
    var pendingEvents = inputEvents.sorted { $0.0 < $1.0 }
    while Date() < deadline {
        let elapsed = Date().timeIntervalSince(start)
        while let event = pendingEvents.first, event.0 <= elapsed {
            sendText(event.1, to: surface)
            pendingEvents.removeFirst()
        }
        ghostty_app_tick(app)
        ghostty_surface_draw(surface)
        RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.01))
    }
}

func makeShellCommand(_ command: [String]) -> String {
    func quote(_ part: String) -> String {
        "'" + part.replacingOccurrences(of: "'", with: "'\\''") + "'"
    }
    return command.map(quote).joined(separator: " ")
}

func cgImage(from layer: CALayer) throws -> CGImage {
    guard let contents = layer.contents else {
        throw CaptureError.missingLayerContents
    }

    let cfContents = contents as CFTypeRef
    if CFGetTypeID(cfContents) == CGImage.typeID {
        return contents as! CGImage
    }

    if CFGetTypeID(cfContents) == IOSurfaceGetTypeID() {
        let surface = contents as! IOSurface
        let ciImage = CIImage(ioSurface: surface)
        guard let cgImage = CIContext().createCGImage(ciImage, from: ciImage.extent) else {
            throw CaptureError.imageCreate
        }
        return cgImage
    }

    throw CaptureError.unsupportedLayerContents(contents)
}

func writePNG(_ image: CGImage, to output: String) throws {
    let url = URL(fileURLWithPath: output)
    try FileManager.default.createDirectory(
        at: url.deletingLastPathComponent(),
        withIntermediateDirectories: true
    )
    guard let destination = CGImageDestinationCreateWithURL(
        url as CFURL,
        UTType.png.identifier as CFString,
        1,
        nil
    ) else {
        throw CaptureError.imageWrite(url)
    }
    CGImageDestinationAddImage(destination, image, nil)
    guard CGImageDestinationFinalize(destination) else {
        throw CaptureError.imageWrite(url)
    }
}

func withCString<T>(_ value: String, _ body: (UnsafePointer<CChar>) throws -> T) rethrows -> T {
    try value.withCString { pointer in
        try body(pointer)
    }
}

func main() throws {
    let options = try parseOptions()
    guard let output = options.output else {
        throw CaptureError.usage("--output is required")
    }

    var argvStorage = CommandLine.arguments.map { strdup($0) }
    defer { argvStorage.forEach { free($0) } }
    let initResult = ghostty_init(UInt(argvStorage.count), &argvStorage)
    guard initResult == 0 else {
        throw CaptureError.ghosttyInit(initResult)
    }

    NSApplication.shared.setActivationPolicy(.prohibited)

    guard let config = ghostty_config_new() else {
        throw CaptureError.configCreate
    }
    defer { ghostty_config_free(config) }
    ghostty_config_load_default_files(config)
    ghostty_config_load_recursive_files(config)
    ghostty_config_finalize(config)

    var runtime = ghostty_runtime_config_s(
        userdata: nil,
        supports_selection_clipboard: false,
        wakeup_cb: { _ in },
        action_cb: { _, _, _ in false },
        read_clipboard_cb: { _, _, _ in false },
        confirm_read_clipboard_cb: { _, _, _, _ in },
        write_clipboard_cb: { _, _, _, _, _ in },
        close_surface_cb: { _, _ in }
    )

    guard let app = ghostty_app_new(&runtime, config) else {
        throw CaptureError.appCreate
    }
    ghostty_app_set_focus(app, true)

    let frame = NSRect(x: -10000, y: -10000, width: options.width, height: options.height)
    let window = NSWindow(contentRect: frame, styleMask: [.borderless], backing: .buffered, defer: false)
    window.isReleasedWhenClosed = false
    window.backgroundColor = .black
    window.isOpaque = true
    let view = CaptureView(frame: NSRect(x: 0, y: 0, width: options.width, height: options.height))
    view.autoresizingMask = [.width, .height]
    window.contentView = view
    window.orderFrontRegardless()

    let scale = NSScreen.main?.backingScaleFactor ?? 2.0
    let commandString = makeShellCommand(options.command)
    let commandCString = strdup(commandString)
    let cwdCString = strdup(options.cwd)
    let initialInputCString = options.initialInput.map { strdup($0)! }
    let envKey = strdup("TERM")
    let envValue = strdup("xterm-ghostty")
    defer {
        free(commandCString)
        free(cwdCString)
        if let initialInputCString {
            free(initialInputCString)
        }
        free(envKey)
        free(envValue)
    }
    let envVars = UnsafeMutablePointer<ghostty_env_var_s>.allocate(capacity: 1)
    envVars.initialize(to: ghostty_env_var_s(key: UnsafePointer(envKey), value: UnsafePointer(envValue)))
    defer {
        envVars.deinitialize(count: 1)
        envVars.deallocate()
    }

    var surfaceConfig = ghostty_surface_config_new()
    surfaceConfig.platform_tag = GHOSTTY_PLATFORM_MACOS
    surfaceConfig.platform = ghostty_platform_u(macos: ghostty_platform_macos_s(nsview: Unmanaged.passUnretained(view).toOpaque()))
    surfaceConfig.userdata = nil
    surfaceConfig.scale_factor = scale
    surfaceConfig.font_size = options.fontSize
    surfaceConfig.working_directory = UnsafePointer(cwdCString)
    surfaceConfig.command = UnsafePointer(commandCString)
    surfaceConfig.env_vars = envVars
    surfaceConfig.env_var_count = 1
    surfaceConfig.initial_input = initialInputCString.map { UnsafePointer($0) }
    surfaceConfig.wait_after_command = false
    surfaceConfig.context = GHOSTTY_SURFACE_CONTEXT_WINDOW

    guard let surface = ghostty_surface_new(app, &surfaceConfig) else {
        throw CaptureError.surfaceCreate
    }

    ghostty_surface_set_content_scale(surface, scale, scale)
    ghostty_surface_set_size(surface, UInt32(options.width * scale), UInt32(options.height * scale))
    ghostty_surface_set_focus(surface, true)
    ghostty_surface_set_occlusion(surface, true)

    runMainLoop(
        until: Date().addingTimeInterval(options.settleSeconds),
        app: app,
        surface: surface,
        inputEvents: options.inputEvents
    )
    ghostty_surface_draw(surface)
    RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.2))

    guard let layer = view.layer else {
        throw CaptureError.missingLayer
    }
    let image = try cgImage(from: layer)
    try writePNG(image, to: output)
}

do {
    try main()
} catch {
    FileHandle.standardError.write(Data("error: \(error)\n".utf8))
    exit(1)
}
