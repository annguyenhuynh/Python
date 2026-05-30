🕒 What resample() Does

* resample() is used to change the frequency of your time series data — similar to how you might group by time periods.

* You can use it to:
    * Downsample (e.g., daily → monthly or weekly)
    * Upsample (e.g., monthly → daily, filling missing data)
    * It works only on a **DatetimeIndex, PeriodIndex, or TimedeltaIndex**

🧠 Basic Syntax
* df.resample(rule, on=None).<aggregation>()
* Parameters:
    * rule → frequency string (e.g., 'D', 'M', 'W', 'H', 'Q')
    * on → optional column name if your datetime isn’t the index
    * <aggregation> → function like sum(), mean(), count(), etc.