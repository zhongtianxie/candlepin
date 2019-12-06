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

import liquibase.Scope;
import liquibase.change.custom.CustomTaskChange;
import liquibase.exception.CustomChangeException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;



/**
 * The AbstractLiquibaseTask class provides the standardized interface and framework for creating
 * Candlepin-specific custom Liquibase tasks.
 */
public abstract class AbstractLiquibaseTask implements CustomTaskChange {

    protected LiquibaseTaskLogger log;
    protected ResourceAccessor resourceAccessor;

    protected AbstractLiquibaseTask() {
        // Intentionally left empty
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFileOpener(ResourceAccessor accessor) {
        this.resourceAccessor = accessor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setUp() {
        this.log = new LiquibaseTaskLogger(Scope.getCurrentScope().getLog(this.getClass()));

        // Add any other startup tasks here

        this.init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ValidationErrors validate(liquibase.database.Database lbdb) {
        this.log = new LiquibaseTaskLogger(Scope.getCurrentScope().getLog(this.getClass()));

        try (Database cpdb = new Database(lbdb)) {
            return this.validate(cpdb);
        }
        catch (Exception exception) {
            throw new UnexpectedLiquibaseException(exception);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(liquibase.database.Database lbdb) throws CustomChangeException {
        try (Database cpdb = new Database(lbdb)) {
            this.execute(cpdb);
        }
        catch (Exception exception) {
            throw new CustomChangeException(exception);
        }
    }

    /**
     * Called after the internal setup tasks are performed, after task instantiation but before
     * validation or execution. Any task-specific setup operations should be performed here.
     */
    public abstract void init();

    /**
     * Called after initialization but before execution. Any errors detected during validation
     * should be added to a single ValidationError instance to be returned at the end of the
     * validation step.
     *
     * @param database
     *  The database on which to perform validation
     *
     * @return
     *  The ValidationErrors that occurred during validation, or null if no validation errors
     *  occurred
     */
    public abstract ValidationErrors validate(Database database) throws Exception;

    /**
     * Called after initialization and validation. Performs the custom task operations. If any
     * operation cannot be completed successfully, this method should throw a LiquibaseException.
     *
     * @param database
     *  The database on which to perform validation
     *
     * @throws Exception
     *  if a checked exception occurs during task execution
     */
    public abstract void execute(Database database) throws Exception;

}
