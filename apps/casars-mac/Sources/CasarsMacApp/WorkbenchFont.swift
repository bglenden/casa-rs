import SwiftUI

enum WorkbenchFontRole {
    case caption2
    case caption
    case subheadline
    case body
    case headline
    case title3
    case title2
    case largeTitle

    func pointSize(base: Double) -> CGFloat {
        let offset: Double = switch self {
        case .caption2: -3
        case .caption: -2
        case .subheadline: -1
        case .body: 0
        case .headline: 1
        case .title3: 4
        case .title2: 7
        case .largeTitle: 18
        }
        return CGFloat(max(8, base + offset))
    }
}

private struct WorkbenchFontSizeKey: EnvironmentKey {
    static let defaultValue = 13.0
}

extension EnvironmentValues {
    var workbenchFontSize: Double {
        get { self[WorkbenchFontSizeKey.self] }
        set { self[WorkbenchFontSizeKey.self] = newValue }
    }
}

private struct WorkbenchFontModifier: ViewModifier {
    @Environment(\.workbenchFontSize) private var baseSize

    let role: WorkbenchFontRole
    let weight: Font.Weight?
    let design: Font.Design

    func body(content: Content) -> some View {
        content.font(.system(size: role.pointSize(base: baseSize), weight: weight, design: design))
    }
}

extension View {
    func workbenchFont(
        _ role: WorkbenchFontRole,
        weight: Font.Weight? = nil,
        design: Font.Design = .default
    ) -> some View {
        modifier(WorkbenchFontModifier(role: role, weight: weight, design: design))
    }
}
