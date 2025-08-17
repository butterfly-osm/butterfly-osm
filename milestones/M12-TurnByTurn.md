# M12 — Turn-by-Turn Instructions (4 micro-milestones)

## M12.1 — Geometry Unpacking
**Why**: Efficient geometry access for navigation
**Artifacts**: `nav.simpl` chunk reader, lazy geometry mapping, geometry cache
**Performance**: Geometry unpack adds ≤1ms median to routing time
**Commit**: `"M12.1: geometry unpacking"`

## M12.2 — Name Dictionary
**Why**: Street names for turn instructions
**Artifacts**: `names.dict` compressed storage, name lookup cache
**Commit**: `"M12.2: name dictionary"`

## M12.3 — Instruction Generation
**Why**: Human-readable turn-by-turn directions
**Artifacts**: Turn instruction logic, maneuver detection, distance/bearing calculation
**Quality**: ETA parity with time-only ≤0.5s
**Commit**: `"M12.3: instruction generation"`

## M12.4 — Steps API
**Why**: Navigation endpoint
**Artifacts**: `/route?steps=true` endpoint, instruction formatting
**Commit**: `"M12.4: steps API"`

**🔄 PRS v9**: Turn-by-turn correctness + ETA parity + geometry performance + **FULL SERVICE PRODUCTION-READY**