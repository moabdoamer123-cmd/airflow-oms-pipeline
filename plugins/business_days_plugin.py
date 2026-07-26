from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from pendulum import DateTime

class BusinessDaysOnlyTimetable(Timetable):
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if last_automated_data_interval is not None:
            next_start = last_automated_data_interval.end
        else:
            next_start = restriction.earliest
            if next_start is None:
                return None

        while next_start.is_weekend():
            next_start = next_start.add(days=1)

        next_end = next_start.add(days=1)
        return DagRunInfo.interval(start=next_start, end=next_end)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        start = run_after.subtract(days=1)
        return DataInterval(start=start, end=run_after)

class BusinessDaysPlugin(AirflowPlugin):
    name = "business_days_plugin"
    timetables = [BusinessDaysOnlyTimetable]