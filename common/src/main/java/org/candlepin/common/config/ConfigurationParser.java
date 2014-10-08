/**
 * Copyright (c) 2009 - 2012 Red Hat, Inc.
 *
 * This software is licensed to you under the GNU General Public License,
 * version 2 (GPLv2). There is NO WARRANTY for this software, express or
 * implied, including the implied warranties of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. You should have received a copy of GPLv2
 * along with this software; if not, see
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.
 *
 * Red Hat trademarks are not licensed under GPLv2. No permission is
 * granted to use or replicate Red Hat trademarks that are incorporated
 * in this software or its documentation.
 */

package org.candlepin.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * ConfigurationParser will take a given prefix and pull out that section of
 * the configuration as a Properties object. This class needs to be extended
 * and the getPrefix method should be overridden.
 */
abstract class ConfigurationParser {

    private static Logger log = LoggerFactory.getLogger(ConfigurationParser.class);

    /**
     * Converts the given Map into a Properties object.
     *
     * @param inputConfiguration Configuration to be converted.
     * @return config as a Properties file
     */
    public Properties parseConfig(Map<String, String> inputConfiguration) {
        Properties toReturn = new Properties();
        toReturn.putAll(stripPrefixFromConfigKeys(inputConfiguration));
        return toReturn;
    }

    /**
     * Return a copy of the input without the prefixes.
     *
     * @param inputConfiguration Configuration to be converted.
     * @return config as a Properties object without the prefixes.
     */
    public Properties stripPrefixFromConfigKeys(
        Map<String, String> inputConfiguration) {
        Properties toReturn = new Properties();

        for (Entry<String, String> entry : inputConfiguration.entrySet()) {
            if (entry.getKey().startsWith(getPrefix())) {
                toReturn.put(entry.getKey().substring(getPrefix().length() + 1),
                    entry.getValue());
            }
            else {
                toReturn.put(entry.getKey(), entry.getValue());
            }
        }

        return toReturn;
    }

    public abstract String getPrefix();
}
