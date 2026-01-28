//! Test fixtures for REST API testing.
//!
//! Provides predefined FHIR resources for use in tests.

use serde_json::{json, Value};

/// Collection of test fixtures.
#[derive(Debug, Clone, Default)]
pub struct TestFixtures {
    /// Patient fixtures.
    pub patients: Vec<PatientFixture>,
    /// Observation fixtures.
    pub observations: Vec<ObservationFixture>,
    /// Organization fixtures.
    pub organizations: Vec<OrganizationFixture>,
}

impl TestFixtures {
    /// Creates empty fixtures.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a minimal set of fixtures.
    pub fn minimal() -> Self {
        Self {
            patients: vec![
                PatientFixture::new("patient-1", "Smith").with_given(vec!["John"]),
            ],
            observations: vec![
                ObservationFixture::new("obs-1", "8867-4", "Patient/patient-1")
                    .with_value(72.0, "bpm"),
            ],
            organizations: vec![
                OrganizationFixture::new("org-1", "Test Hospital"),
            ],
        }
    }

    /// Creates a rich set of fixtures.
    pub fn rich() -> Self {
        Self {
            patients: vec![
                PatientFixture::new("patient-1", "Smith")
                    .with_given(vec!["John"])
                    .with_birth_date("1980-01-15")
                    .with_gender("male"),
                PatientFixture::new("patient-2", "Jones")
                    .with_given(vec!["Jane"])
                    .with_birth_date("1990-05-20")
                    .with_gender("female"),
                PatientFixture::new("patient-3", "Williams")
                    .with_given(vec!["Robert"])
                    .with_active(false),
            ],
            observations: vec![
                ObservationFixture::new("obs-1", "8867-4", "Patient/patient-1")
                    .with_display("Heart rate")
                    .with_value(72.0, "bpm"),
                ObservationFixture::new("obs-2", "8310-5", "Patient/patient-1")
                    .with_display("Body temperature")
                    .with_value(37.0, "Cel"),
                ObservationFixture::new("obs-3", "8867-4", "Patient/patient-2")
                    .with_display("Heart rate")
                    .with_value(68.0, "bpm"),
            ],
            organizations: vec![
                OrganizationFixture::new("org-1", "General Hospital"),
                OrganizationFixture::new("org-2", "City Clinic"),
            ],
        }
    }

    /// Returns all resources as (type, id, json) tuples.
    pub fn all_resources(&self) -> Vec<(&str, String, Value)> {
        let mut resources = Vec::new();

        for p in &self.patients {
            resources.push(("Patient", p.id.clone(), p.to_json()));
        }
        for o in &self.observations {
            resources.push(("Observation", o.id.clone(), o.to_json()));
        }
        for o in &self.organizations {
            resources.push(("Organization", o.id.clone(), o.to_json()));
        }

        resources
    }
}

/// A patient fixture.
#[derive(Debug, Clone)]
pub struct PatientFixture {
    pub id: String,
    pub family: String,
    pub given: Vec<String>,
    pub birth_date: Option<String>,
    pub gender: Option<String>,
    pub active: bool,
}

impl PatientFixture {
    /// Creates a new patient fixture.
    pub fn new(id: impl Into<String>, family: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            family: family.into(),
            given: vec![],
            birth_date: None,
            gender: None,
            active: true,
        }
    }

    /// Sets given names.
    pub fn with_given(mut self, given: Vec<&str>) -> Self {
        self.given = given.into_iter().map(String::from).collect();
        self
    }

    /// Sets birth date.
    pub fn with_birth_date(mut self, date: impl Into<String>) -> Self {
        self.birth_date = Some(date.into());
        self
    }

    /// Sets gender.
    pub fn with_gender(mut self, gender: impl Into<String>) -> Self {
        self.gender = Some(gender.into());
        self
    }

    /// Sets active status.
    pub fn with_active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    /// Converts to FHIR JSON.
    pub fn to_json(&self) -> Value {
        let mut patient = json!({
            "resourceType": "Patient",
            "id": self.id,
            "active": self.active,
            "name": [{
                "family": self.family,
                "given": self.given,
            }],
        });

        if let Some(birth_date) = &self.birth_date {
            patient["birthDate"] = json!(birth_date);
        }
        if let Some(gender) = &self.gender {
            patient["gender"] = json!(gender);
        }

        patient
    }
}

/// An observation fixture.
#[derive(Debug, Clone)]
pub struct ObservationFixture {
    pub id: String,
    pub code: String,
    pub code_display: String,
    pub patient_ref: String,
    pub status: String,
    pub value: Option<f64>,
    pub unit: Option<String>,
}

impl ObservationFixture {
    /// Creates a new observation fixture.
    pub fn new(id: impl Into<String>, code: impl Into<String>, patient_ref: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            code: code.into(),
            code_display: String::new(),
            patient_ref: patient_ref.into(),
            status: "final".to_string(),
            value: None,
            unit: None,
        }
    }

    /// Sets display text.
    pub fn with_display(mut self, display: impl Into<String>) -> Self {
        self.code_display = display.into();
        self
    }

    /// Sets a quantity value.
    pub fn with_value(mut self, value: f64, unit: impl Into<String>) -> Self {
        self.value = Some(value);
        self.unit = Some(unit.into());
        self
    }

    /// Converts to FHIR JSON.
    pub fn to_json(&self) -> Value {
        let mut obs = json!({
            "resourceType": "Observation",
            "id": self.id,
            "status": self.status,
            "code": {
                "coding": [{
                    "system": "http://loinc.org",
                    "code": self.code,
                    "display": self.code_display,
                }],
            },
            "subject": {
                "reference": self.patient_ref,
            },
        });

        if let (Some(value), Some(unit)) = (&self.value, &self.unit) {
            obs["valueQuantity"] = json!({
                "value": value,
                "unit": unit,
            });
        }

        obs
    }
}

/// An organization fixture.
#[derive(Debug, Clone)]
pub struct OrganizationFixture {
    pub id: String,
    pub name: String,
    pub active: bool,
}

impl OrganizationFixture {
    /// Creates a new organization fixture.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            active: true,
        }
    }

    /// Sets active status.
    pub fn with_active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    /// Converts to FHIR JSON.
    pub fn to_json(&self) -> Value {
        json!({
            "resourceType": "Organization",
            "id": self.id,
            "name": self.name,
            "active": self.active,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_patient_to_json() {
        let patient = PatientFixture::new("123", "Smith")
            .with_given(vec!["John"])
            .with_birth_date("1980-01-01");

        let json = patient.to_json();

        assert_eq!(json["resourceType"], "Patient");
        assert_eq!(json["id"], "123");
        assert_eq!(json["name"][0]["family"], "Smith");
        assert_eq!(json["birthDate"], "1980-01-01");
    }

    #[test]
    fn test_observation_to_json() {
        let obs = ObservationFixture::new("obs-1", "8867-4", "Patient/123")
            .with_value(72.0, "bpm");

        let json = obs.to_json();

        assert_eq!(json["resourceType"], "Observation");
        assert_eq!(json["valueQuantity"]["value"], 72.0);
    }

    #[test]
    fn test_fixtures_all_resources() {
        let fixtures = TestFixtures::minimal();
        let resources = fixtures.all_resources();

        assert_eq!(resources.len(), 3);
    }
}
