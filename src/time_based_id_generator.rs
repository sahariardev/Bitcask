use chrono::{DateTime, Utc};

pub struct SystemClock;

impl SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
pub struct TimeBasedIdGenerator {
    clock: SystemClock,
}

impl TimeBasedIdGenerator {
    pub fn new() -> Self {
        TimeBasedIdGenerator {
            clock: SystemClock {},
        }
    }

    pub fn next(&self) -> u64 {
        self.clock.now().timestamp() as u64
    }
}
