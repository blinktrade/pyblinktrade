class ProjectOptions(object):
  def __init__(self, config, section):
    self.config = config
    self.section = section

    def make_getters(tag):
      @property
      def _getter(self):
        raw_str = self.config.get(self.section, tag)
        try:
          return self.config.getint(self.section, tag)
        except Exception:
          pass
        try:
          return self.config.getfloat(self.section, tag)
        except Exception:
          pass
        try:
          return self.config.getboolean(self.section, tag)
        except Exception:
          pass

        return raw_str
      return _getter

    for k,v in self.items():
      _getter = make_getters(k)
      setattr(ProjectOptions, k ,_getter)

  def has_option(self, attribute):
    return self.config.has_option(self.section, attribute)
  def get(self, attribute):
    return self.config.get(self.section, attribute)
  def getint(self, attribute):
    return self.config.getint(self.section, attribute)
  def getfloat(self, attribute):
    return self.config.getfloat(self.section, attribute)
  def getboolean(self, attribute):
    return self.config.getboolean(self.section, attribute)
  def items(self):
    return self.config.items(self.section)
  def options(self):
    return self.config.options(self.section)