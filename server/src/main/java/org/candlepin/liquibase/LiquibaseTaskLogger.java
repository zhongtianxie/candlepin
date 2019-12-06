/**
 * Copyright (c) 2009 - 2019 Red Hat, Inc.
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
package org.candlepin.liquibase;

import liquibase.logging.Logger;



/**
 * The LiquibaseTaskLogger is a simple abstraction around the Liquibase Logger, providing some
 * utility functionality such as printf-style logging methods and a helper method for checking the
 * current log level against a target log level.
 */
public class LiquibaseTaskLogger {

    protected Logger logger;

    public LiquibaseTaskLogger(Logger logger) {
        if (logger == null) {
            throw new IllegalArgumentException("logger is null");
        }

        this.logger = logger;
    }

    protected String compileMessage(String message, Object... params) {
        if (message != null) {
            // TODO: Add i18n translation here

            if (params != null && params.length > 0) {
                message = String.format(message, params);
            }
        }

        return message;
    }

    public void debug(String message, Object... params) {
        this.logger.debug(this.compileMessage(message, params));
    }

    public void info(String message, Object... params) {
        this.logger.info(this.compileMessage(message, params));
    }

    public void warn(String message, Object... params) {
        this.logger.warning(this.compileMessage(message, params));
    }

    public void error(String message, Object... params) {
        this.logger.severe(this.compileMessage(message, params));
    }

    // TODO: Reenable this in the future if Liquibase ever stops breaking their own API

    // public LogLevel getLogLevel() {
    //     return this.logger.getLogLevel();
    // }

    // public boolean isLogLevelEnabled(LogLevel level) {
    //     if (level == null) {
    //         throw new IllegalArgumentException("level is null");
    //     }

    //     LogLevel current = this.getLogLevel();
    //     return (current != null && current.compareTo(level) <= 0);
    // }

}
