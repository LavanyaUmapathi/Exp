# UTC Timezone

from __future__ import print_function

# Standard python modules
import datetime


# Python makes this so damn hard
ZERO = datetime.timedelta(0)

class UTC(datetime.tzinfo):
    zone = "UTC"

    def fromutc(self, dt):
      if dt.tzinfo is None:
        return self.localize(dt)
      return super(utc.__class__, self).fromutc(dt)

    def utcoffset(self, dt):
        return ZERO

    def dst(self, dt):
        return ZERO

    def __reduce__(self):
        return _UTC, ()

    def localize(self, dt, is_dst=False):
        if dt.tzinfo is not None:
            raise ValueError('Not naive datetime (tzinfo is already set)')
        return dt.replace(tzinfo=self)

    def normalize(self, dt, is_dst=False):
        '''Correct the timezone information on the given datetime'''
        if dt.tzinfo is self:
            return dt
        if dt.tzinfo is None:
            raise ValueError('Naive time - no tzinfo set')
        return dt.astimezone(self)

    def tzname(self, dt):
        return self.zone

    def __repr__(self):
        return "<UTC>"

    def __str__(self):
        return self.zone


UTC = utc = UTC() # UTC is a singleton

def _UTC():
    return utc

_UTC.__safe_for_unpickling__ = True
