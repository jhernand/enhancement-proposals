# Request MFA Usage from Brokered OIDC Server

| Field | Value |
|---|---|
| Author(s) | Juan Hernandez |
| Jira | [OSAC-3027](https://redhat.atlassian.net/browse/OSAC-3027) |
| Date | 2026-10-29 |
| Target milestone | 0.5 |

## Problem Statement

Today, OSAC sign-in through a tenant-managed OpenID Connect (OIDC) server does
not request multifactor authentication (MFA). Cloud Infrastructure Admins
cannot enable a deployment-wide MFA request, and Tenant Admins lack guidance
on when OSAC will request MFA so they can configure their identity provider.
Without an MFA-specific request from OSAC, a tenant-managed identity provider
has no OSAC signal to use when applying its own MFA policy; it remains
responsible for deciding whether MFA is performed.

## In Scope

- The MFA request is a single deployment-wide policy for every brokered OIDC
  sign-in when enabled; it is not configurable per tenant. [Clarify: R1.Q1]
- A Cloud Infrastructure Admin can configure the setting during installation
  through the Enclave Wizard and can change it after installation. New
  installations default to disabled; existing installations retain their
  current sign-in behavior until the Admin enables it. [Clarify: R2.Q1,
  R2.Q2, R2.Q3, R3.Q2, R3.Q3]
- When enabled, OSAC requests generic MFA from the tenant-managed identity
  provider without specifying a particular method or assurance level. If the
  provider does not apply MFA, sign-in follows that provider's own policy. When
  disabled, OSAC sends no MFA request and brokered sign-in behavior remains
  unchanged. [Clarify: R1.Q2, R1.Q3, R3.Q1]
- Tenant Admin guidance explains when OSAC sends the request and that the
  tenant-managed identity provider decides whether MFA is performed. OSAC does
  not guarantee or enforce MFA. [Jira: OSAC-3027] [Clarify: R1.Q3, R1.Q5]
- A Cloud Infrastructure Admin can verify during a brokered OIDC sign-in that
  OSAC sends the MFA request; per-sign-in evidence is not required. [Jira:
  OSAC-3027] [Clarify: R1.Q5]

## Out of Scope

- Platform-enforced MFA, including blocking sign-in when the identity provider
  does not apply MFA.
- Changes to MFA policy on tenant-managed identity providers, and
  tenant-specific request settings.
- Per-sign-in evidence or reporting that claims MFA was performed.
- Separate MFA configuration controls in the OSAC console or CLI; the
  installation-time Enclave Wizard control is in scope.
- Automatic session logoff and configurable session timeouts.

## User Stories

### Cloud Provider Admin

- As a Cloud Provider Admin, I want OSAC to request generic MFA from all
  brokered identity providers when the deployment setting is enabled, so that
  an identity provider can apply its MFA policy. [Clarify: R1.Q1, R1.Q2]

### Cloud Infrastructure Admin

- As a Cloud Infrastructure Admin, I want to enable or disable the global MFA
  request during installation and change it later, so that I control whether
  OSAC requests MFA during brokered sign-in. [Clarify: R2.Q1, R2.Q2, R2.Q3,
  R3.Q1]
- As a Cloud Infrastructure Admin, I want to verify during a brokered OIDC
  sign-in that OSAC sends the request, so that I can validate the
  authentication setup. [Jira: OSAC-3027]

### Tenant Admin

- As a Tenant Admin, I want guidance on when OSAC requests MFA and what my
  identity provider decides, so that I can configure my tenant-managed
  provider accordingly. [Jira: OSAC-3027] [Clarify: R1.Q3, R1.Q5]

### Tenant User

- As a Tenant User, I want my sign-in to follow my organization's MFA policy
  when the identity provider honors OSAC's request, so that authentication
  follows my organization's policy. [Jira: OSAC-3027] [Clarify: R1.Q2,
  R1.Q3]

## Dependencies

- **Enclave Wizard:** The installation flow must surface the global setting for
  the Cloud Infrastructure Admin. [Clarify: R2.Q2]
- [**OSAC-3024 — Red Hat Sovereign Enclave Integration**](https://redhat.atlassian.net/browse/OSAC-3024):
  Jira links OSAC-3027 as depending on this Feature.
- [**OSAC-4387 — Token Lifecycle Hardening**](https://redhat.atlassian.net/browse/OSAC-4387):
  Jira marks OSAC-3027 as blocked by this Epic.
- **Downstream linked work:** [OSAC-3018 — HIPAA Compliance Readiness](https://redhat.atlassian.net/browse/OSAC-3018),
  [OSAC-3019 — NIST SP 800-53 Compliance Readiness](https://redhat.atlassian.net/browse/OSAC-3019), and
  [OSAC-2889 — EU Sovereignty & Compliance Readiness](https://redhat.atlassian.net/browse/OSAC-2889)
  are linked as depending on OSAC-3027.

---

## Provenance

Authored: revise @ prd 0.11.5 - 7b2b8de, workspace main @ 6f3f72851
Phases: draft, revise

<!-- ai-workflow-provenance:{"schema_version":1,"provenance_kind":"session","workflow":"prd","workflow_version":"0.11.5","ai_workflows":"7b2b8de","source_repo":"6f3f72851","source_repo_branch":"main","commits_behind_main":0,"commits_ahead_main":0,"main_ref":"main","phases":["draft","revise"],"authoring_modes":["skill"],"context_changed":false,"origin_untracked":false} -->
