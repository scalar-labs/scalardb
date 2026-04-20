package com.scalar.db;

import com.github.jk1.license.ConfigurationData;
import com.github.jk1.license.ModuleData;
import com.github.jk1.license.ProjectData;
import com.github.jk1.license.filter.DependencyFilter;

/**
 * A dependency filter that removes manifest and license file data from modules, keeping only
 * POM-derived license information.
 */
public class PomOnlyFilter implements DependencyFilter {

  @Override
  public ProjectData filter(ProjectData source) {
    for (ConfigurationData configuration : source.getConfigurations()) {
      for (ModuleData module : configuration.getDependencies()) {
        module.getManifests().clear();
        module.getLicenseFiles().clear();
      }
    }
    return source;
  }

  @Override
  public String toString() {
    return "PomOnlyFilter";
  }
}
