import AppKit
import SwiftUI

// PROTOTYPE — throwaway first-run onboarding study.
// Three variants of the empty Workbench, switchable in place with the bottom evaluator bar.
struct FirstRunOnboardingPrototypeView: View {
    @State private var variant = FirstRunOnboardingVariant.initial
    @State private var lastAction = "none"
    @State private var guidedStep = 1

    var body: some View {
        ZStack(alignment: .bottom) {
            switch variant {
            case .welcomeSheet:
                WelcomeSheetVariant(lastAction: $lastAction)
            case .pathLaunchpad:
                PathLaunchpadVariant(lastAction: $lastAction)
            case .guidedSetup:
                GuidedSetupVariant(
                    step: $guidedStep,
                    lastAction: $lastAction
                )
            }

            PrototypeVariantSwitcher(
                variant: $variant,
                lastAction: lastAction
            )
            .padding(.bottom, 18)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(nsColor: .textBackgroundColor))
        .accessibilityIdentifier("onboardingPrototype.root")
    }
}

private enum FirstRunOnboardingVariant: String, CaseIterable, Identifiable {
    case welcomeSheet = "A"
    case pathLaunchpad = "B"
    case guidedSetup = "C"

    var id: String { rawValue }

    var title: String {
        switch self {
        case .welcomeSheet: "Welcome sheet"
        case .pathLaunchpad: "Choose your path"
        case .guidedSetup: "Guided setup"
        }
    }

    static var initial: Self {
        guard let index = CommandLine.arguments.firstIndex(of: "--prototype-variant"),
              CommandLine.arguments.indices.contains(index + 1),
              let variant = Self(rawValue: CommandLine.arguments[index + 1].uppercased())
        else {
            return .welcomeSheet
        }
        return variant
    }
}

private struct WelcomeSheetVariant: View {
    @Binding var lastAction: String

    var body: some View {
        ZStack {
            CurrentEmptyStateBackdrop()
                .blur(radius: 1.2)

            Color.black.opacity(0.42)

            VStack(alignment: .leading, spacing: 18) {
                HStack(alignment: .top, spacing: 16) {
                    Image(systemName: "sparkles.rectangle.stack.fill")
                        .font(.system(size: 34))
                        .foregroundStyle(.blue)
                        .frame(width: 54, height: 54)
                        .background(Color.blue.opacity(0.12))
                        .clipShape(RoundedRectangle(cornerRadius: 14))

                    VStack(alignment: .leading, spacing: 5) {
                        HStack {
                            Text("Welcome to casa-rs")
                                .font(.system(size: 25, weight: .semibold))
                            Spacer()
                            PrototypeDisclosure(variant: "A · HYBRID")
                        }
                        Text("Explore radio astronomy data in a workspace that keeps your data, notes, plots, and CASA-style tasks together.")
                            .foregroundStyle(.secondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }

                VStack(alignment: .leading, spacing: 10) {
                    Text("RECOMMENDED FOR FIRST-TIME USERS")
                        .font(.caption2.weight(.bold))
                        .foregroundStyle(.blue)
                    Label("Learn with the TW Hya tutorial", systemImage: "graduationcap.fill")
                        .font(.title3.weight(.semibold))
                    Text("Create a learner workspace, acquire a real ALMA dataset, inspect it, and run imaging tasks one at a time—with an explanation before every step.")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)

                    Button {
                        lastAction = "start guided tutorial"
                    } label: {
                        Label("Start the guided tutorial", systemImage: "arrow.right.circle.fill")
                            .frame(maxWidth: .infinity)
                    }
                    .buttonStyle(.borderedProminent)
                    .controlSize(.large)
                }
                .padding(16)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(Color.blue.opacity(0.08))
                .clipShape(RoundedRectangle(cornerRadius: 10))
                .overlay(RoundedRectangle(cornerRadius: 10).stroke(Color.blue.opacity(0.22)))

                HStack(alignment: .top, spacing: 10) {
                    WelcomeSecondaryPath(
                        icon: "folder.fill",
                        title: "Open my project",
                        detail: "Choose a directory. Workbench will discover supported data, images, tables, and notebooks."
                    ) {
                        lastAction = "open existing project"
                    }

                    WelcomeSecondaryPath(
                        icon: "shippingbox.fill",
                        title: "Explore a demo",
                        detail: "Tour the interface with bundled sample content and no setup or network access."
                    ) {
                        lastAction = "explore demo"
                    }
                }

                Text("Nothing downloads or runs until you review and approve it.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .center)
            }
            .padding(28)
            .frame(width: 650)
            .background(.thickMaterial)
            .clipShape(RoundedRectangle(cornerRadius: 18))
            .shadow(color: .black.opacity(0.35), radius: 28, y: 14)
            .padding(.bottom, 70)
        }
        .ignoresSafeArea()
    }
}

private struct WelcomeSecondaryPath: View {
    let icon: String
    let title: String
    let detail: String
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            HStack(alignment: .top, spacing: 11) {
                Image(systemName: icon)
                    .font(.title3)
                    .foregroundStyle(.blue)
                    .frame(width: 28)
                VStack(alignment: .leading, spacing: 4) {
                    Text(title)
                        .font(.headline)
                    Text(detail)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
                Spacer(minLength: 4)
                Image(systemName: "chevron.right")
                    .font(.caption.weight(.bold))
                    .foregroundStyle(.secondary)
            }
            .padding(13)
            .frame(maxWidth: .infinity, minHeight: 94, alignment: .topLeading)
            .background(Color(nsColor: .controlBackgroundColor))
            .clipShape(RoundedRectangle(cornerRadius: 10))
        }
        .buttonStyle(.plain)
    }
}

private struct PathLaunchpadVariant: View {
    @Binding var lastAction: String

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 26) {
                PrototypeDisclosure(variant: "B")

                VStack(alignment: .leading, spacing: 8) {
                    Text("What would you like to do?")
                        .font(.system(size: 30, weight: .semibold))
                    Text("Choose a starting point. You can change course at any time.")
                        .font(.title3)
                        .foregroundStyle(.secondary)
                }

                VStack(spacing: 12) {
                    OnboardingPathRow(
                        icon: "graduationcap.fill",
                        tint: .blue,
                        eyebrow: "RECOMMENDED FOR FIRST-TIME USERS",
                        title: "Learn with the TW Hya tutorial",
                        detail: "Create a learner workspace, acquire a real ALMA dataset, inspect it, and run imaging tasks one at a time.",
                        actionTitle: "Start learning"
                    ) {
                        lastAction = "start TW Hya tutorial"
                    }

                    OnboardingPathRow(
                        icon: "folder.fill",
                        tint: .orange,
                        eyebrow: "I HAVE DATA OR AN EXISTING WORKSPACE",
                        title: "Open a casa-rs project",
                        detail: "Choose a directory. Workbench will discover supported MeasurementSets, images, tables, and notebooks.",
                        actionTitle: "Choose directory"
                    ) {
                        lastAction = "choose project directory"
                    }

                    OnboardingPathRow(
                        icon: "shippingbox.fill",
                        tint: .purple,
                        eyebrow: "LOOK AROUND WITHOUT SETTING ANYTHING UP",
                        title: "Explore the demo project",
                        detail: "Tour the interface with bundled sample content. No network access or project setup required.",
                        actionTitle: "Open demo"
                    ) {
                        lastAction = "open demo project"
                    }
                }

                HStack(spacing: 18) {
                    Label("Tasks run only when you click Run", systemImage: "hand.tap")
                    Label("Downloads require approval", systemImage: "checkmark.shield")
                    Label("Your notes stay editable", systemImage: "square.and.pencil")
                }
                .font(.caption)
                .foregroundStyle(.secondary)
            }
            .frame(maxWidth: 820, alignment: .leading)
            .padding(.horizontal, 44)
            .padding(.top, 34)
            .padding(.bottom, 120)
            .frame(maxWidth: .infinity)
        }
    }
}

private struct GuidedSetupVariant: View {
    @Binding var step: Int
    @Binding var lastAction: String

    var body: some View {
        VStack(spacing: 0) {
            HStack(alignment: .center) {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Set up your first workspace")
                        .font(.system(size: 24, weight: .semibold))
                    Text("Three short choices, then you’ll be ready to explore.")
                        .foregroundStyle(.secondary)
                }
                Spacer()
                PrototypeDisclosure(variant: "C")
            }
            .padding(.horizontal, 30)
            .padding(.vertical, 22)

            Divider()

            HStack(alignment: .top, spacing: 0) {
                VStack(alignment: .leading, spacing: 0) {
                    GuidedStepRow(number: 1, title: "Choose an experience", selected: step == 1)
                    GuidedStepConnector(completed: step > 1)
                    GuidedStepRow(number: 2, title: "Choose a workspace", selected: step == 2)
                    GuidedStepConnector(completed: step > 2)
                    GuidedStepRow(number: 3, title: "Review and begin", selected: step == 3)
                }
                .padding(28)
                .frame(width: 270, alignment: .topLeading)

                Divider()

                VStack(alignment: .leading, spacing: 20) {
                    switch step {
                    case 1:
                        Text("How familiar are you with CASA workflows?")
                            .font(.title2.weight(.semibold))
                        GuidedChoice(
                            icon: "sparkles",
                            title: "I’m new — guide me",
                            detail: "Start with explanations, safe defaults, and the TW Hya learning path.",
                            selected: lastAction == "new user path"
                        ) {
                            lastAction = "new user path"
                        }
                        GuidedChoice(
                            icon: "antenna.radiowaves.left.and.right",
                            title: "I know CASA — show me the tools",
                            detail: "Open a project directly and keep guidance available when needed.",
                            selected: lastAction == "experienced user path"
                        ) {
                            lastAction = "experienced user path"
                        }
                    case 2:
                        Text("Where should this work live?")
                            .font(.title2.weight(.semibold))
                        GuidedChoice(
                            icon: "folder.badge.plus",
                            title: "Create a tutorial workspace",
                            detail: "Choose an empty folder for editable notes, downloaded data, plots, and task results.",
                            selected: lastAction == "create tutorial workspace"
                        ) {
                            lastAction = "create tutorial workspace"
                        }
                        GuidedChoice(
                            icon: "folder",
                            title: "Open an existing directory",
                            detail: "Workbench will inspect it before changing anything.",
                            selected: lastAction == "open existing directory"
                        ) {
                            lastAction = "open existing directory"
                        }
                    default:
                        Text("Ready when you are")
                            .font(.title2.weight(.semibold))
                        VStack(alignment: .leading, spacing: 12) {
                            Label("Create an editable learner notebook", systemImage: "checkmark.circle.fill")
                            Label("Show the dataset source, checksum, size, and destination", systemImage: "checkmark.circle.fill")
                            Label("Wait for your approval before downloading", systemImage: "checkmark.circle.fill")
                            Label("Wait for you to run every scientific task", systemImage: "checkmark.circle.fill")
                        }
                        .foregroundStyle(.secondary)
                        Button {
                            lastAction = "begin guided setup"
                        } label: {
                            Label("Create workspace and continue", systemImage: "arrow.right.circle.fill")
                        }
                        .buttonStyle(.borderedProminent)
                        .controlSize(.large)
                    }

                    Spacer()

                    HStack {
                        if step > 1 {
                            Button("Back") { step -= 1 }
                        }
                        Spacer()
                        if step < 3 {
                            Button("Continue") { step += 1 }
                                .buttonStyle(.borderedProminent)
                                .disabled(lastAction == "none")
                        }
                    }
                }
                .padding(34)
                .frame(maxWidth: 650, maxHeight: .infinity, alignment: .topLeading)
            }
            .frame(maxWidth: 940, maxHeight: .infinity)
            .background(Color(nsColor: .controlBackgroundColor).opacity(0.35))
            .clipShape(RoundedRectangle(cornerRadius: 14))
            .padding(.horizontal, 28)
            .padding(.bottom, 108)
        }
    }
}

private struct CurrentEmptyStateBackdrop: View {
    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            Text("Open a casa-rs project")
                .font(.title2.weight(.semibold))
            Text("Choose a directory and casa-rs will probe it for supported datasets.")
                .foregroundStyle(.secondary)
            HStack {
                Label("Open Project Directory", systemImage: "folder")
                Label("Fork Tutorial Template", systemImage: "book")
                    .foregroundStyle(.tertiary)
                Label("Open Demo Project", systemImage: "shippingbox")
            }
            .font(.caption)
        }
        .padding(28)
        .frame(maxWidth: 600, maxHeight: .infinity, alignment: .center)
    }
}

private struct OnboardingPathRow: View {
    let icon: String
    let tint: Color
    let eyebrow: String
    let title: String
    let detail: String
    let actionTitle: String
    let action: () -> Void

    var body: some View {
        HStack(spacing: 18) {
            Image(systemName: icon)
                .font(.system(size: 24))
                .foregroundStyle(tint)
                .frame(width: 48, height: 48)
                .background(tint.opacity(0.12))
                .clipShape(RoundedRectangle(cornerRadius: 12))

            VStack(alignment: .leading, spacing: 5) {
                Text(eyebrow)
                    .font(.caption2.weight(.bold))
                    .foregroundStyle(tint)
                Text(title)
                    .font(.title3.weight(.semibold))
                Text(detail)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Spacer(minLength: 14)

            Button(actionTitle, action: action)
                .buttonStyle(.bordered)
                .controlSize(.large)
        }
        .padding(18)
        .background(Color(nsColor: .controlBackgroundColor))
        .clipShape(RoundedRectangle(cornerRadius: 12))
        .overlay(RoundedRectangle(cornerRadius: 12).stroke(tint.opacity(0.24)))
    }
}

private struct GuidedChoice: View {
    let icon: String
    let title: String
    let detail: String
    let selected: Bool
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            HStack(spacing: 14) {
                Image(systemName: icon)
                    .font(.title2)
                    .foregroundStyle(selected ? Color.white : Color.accentColor)
                    .frame(width: 38)
                VStack(alignment: .leading, spacing: 4) {
                    Text(title).font(.headline)
                    Text(detail)
                        .font(.callout)
                        .foregroundStyle(selected ? Color.white.opacity(0.82) : Color.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
                Spacer()
                Image(systemName: selected ? "checkmark.circle.fill" : "circle")
                    .foregroundStyle(selected ? Color.white : Color.secondary)
            }
            .padding(16)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(selected ? Color.accentColor : Color(nsColor: .controlBackgroundColor))
            .clipShape(RoundedRectangle(cornerRadius: 12))
        }
        .buttonStyle(.plain)
    }
}

private struct GuidedStepRow: View {
    let number: Int
    let title: String
    let selected: Bool

    var body: some View {
        HStack(spacing: 12) {
            Text("\(number)")
                .font(.headline)
                .foregroundStyle(selected ? Color.white : Color.secondary)
                .frame(width: 30, height: 30)
                .background(selected ? Color.accentColor : Color.secondary.opacity(0.14))
                .clipShape(Circle())
            Text(title)
                .font(.headline)
                .foregroundStyle(selected ? Color.primary : Color.secondary)
        }
    }
}

private struct GuidedStepConnector: View {
    let completed: Bool

    var body: some View {
        Rectangle()
            .fill(completed ? Color.accentColor : Color.secondary.opacity(0.2))
            .frame(width: 2, height: 42)
            .padding(.leading, 14)
    }
}

private struct PrototypeDisclosure: View {
    let variant: String

    var body: some View {
        Label("PROTOTYPE · VARIANT \(variant)", systemImage: "testtube.2")
            .font(.caption2.weight(.bold))
            .foregroundStyle(.blue)
            .padding(.horizontal, 9)
            .padding(.vertical, 5)
            .background(Color.blue.opacity(0.09))
            .clipShape(Capsule())
    }
}

private struct PrototypeVariantSwitcher: View {
    @Binding var variant: FirstRunOnboardingVariant
    let lastAction: String

    var body: some View {
        HStack(spacing: 12) {
            Button {
                cycle(by: -1)
            } label: {
                Image(systemName: "arrow.left")
            }
            .keyboardShortcut(KeyEquivalent.leftArrow, modifiers: [])
            .help("Previous variant")

            VStack(spacing: 2) {
                Text("\(variant.rawValue) — \(variant.title)")
                    .font(.caption.weight(.bold))
                Text("first launch · no project · action: \(lastAction) · persistence off")
                    .font(.caption2.monospaced())
                    .foregroundStyle(.secondary)
            }
            .frame(minWidth: 390)

            Button {
                cycle(by: 1)
            } label: {
                Image(systemName: "arrow.right")
            }
            .keyboardShortcut(KeyEquivalent.rightArrow, modifiers: [])
            .help("Next variant")
        }
        .buttonStyle(.borderless)
        .padding(.horizontal, 16)
        .padding(.vertical, 9)
        .background(.ultraThickMaterial)
        .clipShape(Capsule())
        .overlay(Capsule().stroke(Color.white.opacity(0.18)))
        .shadow(color: .black.opacity(0.3), radius: 16, y: 7)
        .accessibilityIdentifier("onboardingPrototype.switcher")
    }

    private func cycle(by offset: Int) {
        let variants = FirstRunOnboardingVariant.allCases
        guard let currentIndex = variants.firstIndex(of: variant) else { return }
        variant = variants[(currentIndex + offset + variants.count) % variants.count]
    }
}
