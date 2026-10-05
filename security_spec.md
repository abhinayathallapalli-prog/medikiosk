# Database Security & Architecture Specification (Ayush Hospital MediKiosk)

## 1. Core Data Invariants & Collections
- **`/test/{testId}`**: Connection health checking & diagnostics pinging.
- **`/users/{userId}`**: User profiles (patients, doctors, administrators).
- **`/patients/{patientId}`**: Patient registry documents created via kiosk registration or ABHA integration.
- **`/cases/{caseId}`**: Clinical intake cases created at the kiosk terminals and processed by attending physicians.
- **`/appointments/{appointmentId}`**: Scheduled or walk-in OPD consultation tickets with queue token tracking.
- **`/clinicalRecords/{recordId}`**: Comprehensive clinical history, Ayush Ashtavidha Pariksha findings, and doctor prescriptions.
- **`/userChats/{chatId}`**: Multi-turn patient assistant and triaging chat sessions.

## 2. Integrity, Identity, and Access Rules
1. **Public/Kiosk Access**:
   - The MediKiosk terminal functions in public hospital reception areas and triage desks.
   - Patients checking in at the physical kiosk terminal can create and update intake cases, appointments, and patient profiles without requiring prior administrative login.
2. **Attending Doctors & Admins**:
   - Attending doctors and administrative users (e.g. `biswasrishikseh606@gmail.com`) can read, list, update, and manage cases, queues, and clinical records.
3. **Data Protection & Sanitization**:
   - Document IDs and keys are checked to prevent injection and unbounded allocations.
   - State updates on clinical cases and appointments preserve existing core identity records while allowing status transitions (`Waiting` -> `In Consultation` -> `Verified`).
