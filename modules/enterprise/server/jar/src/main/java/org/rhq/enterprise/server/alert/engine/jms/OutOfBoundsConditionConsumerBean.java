/*
 * RHQ Management Platform
 * Copyright (C) 2005-2008 Red Hat, Inc.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.rhq.enterprise.server.alert.engine.jms;

import javax.ejb.ActivationConfigProperty;
import javax.ejb.EJB;
import javax.ejb.MessageDriven;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.rhq.enterprise.server.alert.engine.jms.model.OutOfBoundsConditionMessage;
import org.rhq.enterprise.server.measurement.MeasurementBaselineManagerLocal;

/**
 * Use the default message provider
 * 
 * @author Joseph Marques
 */
@MessageDriven(activationConfig = {
    @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
    @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/OutOfBoundsConditionQueue"),
    @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge"),
    @ActivationConfigProperty(propertyName = "subscriptionDurability", propertyValue = "NonDurable") })
public class OutOfBoundsConditionConsumerBean implements MessageListener {
    private final Log log = LogFactory.getLog(OutOfBoundsConditionConsumerBean.class);

    @EJB
    MeasurementBaselineManagerLocal measurementBaselineManager;

    public void onMessage(Message message) {
        try {
            ObjectMessage objectMessage = (ObjectMessage) message;
            Object object = objectMessage.getObject();

            OutOfBoundsConditionMessage outOfBoundsMessage = (OutOfBoundsConditionMessage) object;

            measurementBaselineManager.insertOutOfBoundsMessage(outOfBoundsMessage);
        } catch (Throwable t) {
            log.error("Error persisting OOB: " + t.getMessage(), t);

            try {
                log.error("JMS Message Info: messageId=" + message.getJMSMessageID() + ", redelivered="
                    + message.getJMSRedelivered());
            } catch (JMSException jmse) {
                log.error("JMS Message Info: error reading information from message, exception was: "
                    + jmse.getMessage());
            }
        }
    }
}