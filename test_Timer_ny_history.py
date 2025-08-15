import akvut

logger = akvut.lag_logger('timer.log')
data = akvut.akv_les_web_logs(logger)
print(data.info())

akvut.ny_history(data, logger)