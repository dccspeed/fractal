package io.arabesque.pattern

import io.arabesque.conf.Configuration

class JBlissPatternSpec extends PatternSpec {
    @Override
    Pattern createPattern() {
        Configuration.get().setPatternClass(JBlissPattern.class);
        return Configuration.get().createPattern();
    }
}
