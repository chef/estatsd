-type shp_metric_type() :: 'm' | 'mr' | 'g' | 'h'.

-record(shp_metric, {key         :: binary(),
                     value       :: integer(),
                     type        :: shp_metric_type(),
                     sample_rate :: float() | undefined}).
