TestingClassDict = {}
try:
    from SF_CACHE import SF_CACHE 
    TestingClassDict['SF_CACHE'] = SF_CACHE
except Exception:
    pass
try:
    from SF_NO_CACHE import SF_NO_CACHE 
    TestingClassDict['SF_NO_CACHE'] = SF_NO_CACHE
except Exception:
    pass

