# ARIMA MODEL
* ARIMA stands for AutoRegressive Integrated Moving Average, which is a method used to describe and forecase time series data.
* The first step in doing time series analysis is to plot the dataset to detect any trend, seasonality and/or cyclyes. When a dataset shows trend, contains seasonality or cyclds, it is said to be **non-stationary**.
* ARIMA model works best with **stationary** data as it'd be easier to make forecats and yield more accurate results.
* Hence, if data is non-stationary, we need to detrend and deseasonalize it first before applying the ARIMA model. That said, it is another topic discussed in a more advanced project.
* This project is beginner-friendly and focus on the ARIMA model itself. The data is stationary and ready to use.
* For ARIMA model to work, we need to identify p (Autoregressive), d (Integrated - Differencing), and q (Moving Average). These values can be identified using ACF and PACF plots. The ACF plot helps determine p value while PACF plot helps identify q value. You can use the PDF file attached to detect the pattern of ACF and PACF plot.
* Once you can select the best set of (p,d,q), you can produce an ARIMA model used for future forecasts.
* Please go through the project codes to understand how to apply ARIMA model.
