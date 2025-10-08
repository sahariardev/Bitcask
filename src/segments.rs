use crate::segment::Segment;
use crate::time_based_id_generator::TimeBasedIdGenerator;

struct Segments {
    active_segment: Segment,
    inactive_segments: Vec<Segment>,
    directory: String,
    max_segment_size: u32,
    id_generator: TimeBasedIdGenerator,
}

impl Segments {
    fn maybe_roll_over_segment(
        &self,
        segment: &Segment,
    ) -> Result<Option<Segment>, std::io::Error> {
        if segment.store.current_write_off_set >= self.max_segment_size as i64 {
            let new_segment =
                Segment::new_segment(self.id_generator.next(), self.directory.as_str())?;
            return Ok(Some(new_segment));
        }

        Ok(None)
    }
}
