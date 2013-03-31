# encoding: utf-8
require 'yaml'


TH_SPEC_DIR = File.dirname(__FILE__) unless defined? TH_SPEC_DIR
TH_SPEC_CONFIG = YAML.load_file(File.join(TH_SPEC_DIR, 'config.yml')) unless defined? TH_SPEC_CONFIG
