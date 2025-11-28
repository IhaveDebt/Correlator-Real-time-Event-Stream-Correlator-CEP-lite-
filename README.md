//
// Correlator.swift
// Complex event processing (CEP) toy: detect patterns across event streams with sliding windows
// Swift 5+
//

import Foundation

struct Event {
    let source: String
    let type: String
    let ts: TimeInterval
    let payload: [String: Any]
}

class Window {
    let sizeSec: TimeInterval
    private var events: [Event] = []
    init(size: TimeInterval) { self.sizeSec = size }
    func add(_ e: Event) {
        events.append(e)
        prune()
    }
    private func prune() {
        let cutoff = Date().timeIntervalSince1970 - sizeSec
        events = events.filter { $0.ts >= cutoff }
    }
    func snapshot() -> [Event] { events }
}

class Correlator {
    private var windows: [String: Window] = [:] // per-source window
    private var rules: [Rule] = []
    init() {}
    
    func registerSource(_ id: String, windowSize: TimeInterval) {
        windows[id] = Window(size: windowSize)
    }
    func addRule(_ rule: Rule) { rules.append(rule) }
    func ingest(event: Event) {
        if let w = windows[event.source] { w.add(event) }
        evaluateRules()
    }
    private func evaluateRules() {
        for r in rules {
            if r.evaluate(windows: windows) {
                print("ALERT: rule '\(r.name)' fired at \(Date())")
            }
        }
    }
}

protocol Rule {
    var name: String { get }
    func evaluate(windows: [String: Window]) -> Bool
}

// Example rule: A pattern where source A emitted type X and within window B emitted type Y
class PatternRule: Rule {
    let name: String
    let sourceA: String
    let typeA: String
    let sourceB: String
    let typeB: String
    init(name: String, sourceA: String, typeA: String, sourceB: String, typeB: String) {
        self.name = name; self.sourceA = sourceA; self.typeA = typeA; self.sourceB = sourceB; self.typeB = typeB
    }
    func evaluate(windows: [String : Window]) -> Bool {
        guard let wa = windows[sourceA], let wb = windows[sourceB] else { return false }
        let hasA = wa.snapshot().contains { $0.type == typeA }
        let hasB = wb.snapshot().contains { $0.type == typeB }
        return hasA && hasB
    }
}

// Demo synthetic stream
func demoCorrelator() {
    print("=== Correlator Demo ===")
    let corr = Correlator()
    corr.registerSource("sensorA", windowSize: 8.0)
    corr.registerSource("sensorB", windowSize: 8.0)
    let rule = PatternRule(name: "A_then_B", sourceA: "sensorA", typeA: "temp_spike", sourceB: "sensorB", typeB: "vibration")
    corr.addRule(rule)
    // emit events staggered; some will satisfy rule
    let now = Date().timeIntervalSince1970
    let events = [
        Event(source: "sensorA", type: "temp_spike", ts: now - 2, payload: [:]),
        Event(source: "sensorB", type: "nominal", ts: now - 1, payload: [:]),
        Event(source: "sensorB", type: "vibration", ts: now, payload: [:])
    ]
    for e in events {
        corr.ingest(event: e)
        sleep(1)
    }
}

demoCorrelator()
