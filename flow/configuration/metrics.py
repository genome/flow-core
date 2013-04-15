import statsd


def initialize_metrics(settings):
    statsd_settings = settings.get('statsd_configuration')
    statsd.init_statsd(statsd_settings)
