import typing

t_log_level =  typing.Literal["debug", "info", "warning"]

t_market = typing.Literal["spot", "um", "cm"]

t_spot_interval = typing.Literal["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
t_um_interval = typing.Literal["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
t_cm_interval = typing.Literal["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]

t_interval = typing.Union[t_spot_interval, t_um_interval, t_cm_interval]