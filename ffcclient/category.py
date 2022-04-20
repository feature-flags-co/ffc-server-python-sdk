class Category:
    """
    This class is used only by the internals of the feature flag storage mechanism.
    This type will be passed to the feature flag storage methods;
    its ``name`` property tells the feature store which collection of data is being referenced ("flags", "flag-values", etc.)
    The purpose is for the storage module to store data as completely generic JSON database
    """

    def __init__(self, name, polling_api_url, streaming_api_url):
        self._name = name
        self._polling_api_url = polling_api_url
        self._streaming_api_url = streaming_api_url

    @property
    def name(self):
        return self._name

    @property
    def polling_api_url(self):
        return self._polling_api_url

    @property
    def streaming_api_url(self):
        return self._streaming_api_url


FFC_FEATURE_FLAGS = Category('featureFlags', '/api/public/sdk/latest-feature-flags', '/streaming')

FFC_SEGMENTS = Category('segments', '/api/public/sdk/latest-feature-flags', '/streaming')

FFC_ALL_CATS = [FFC_FEATURE_FLAGS]

FFC_ALL_CAT_NAMES = ['featureFlags', 'segments']
