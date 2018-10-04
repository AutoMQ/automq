/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream;

import org.junit.Test;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SessionWindowsTest {

    @Test
    public void shouldSetWindowGap() {
        final long anyGap = 42L;
        assertEquals(anyGap, SessionWindows.with(ofMillis(anyGap)).inactivityGap());
    }

    @Deprecated
    @Test
    public void shouldSetWindowRetentionTime() {
        final long anyRetentionTime = 42L;
        assertEquals(anyRetentionTime, SessionWindows.with(ofMillis(1)).until(anyRetentionTime).maintainMs());
    }


    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        SessionWindows.with(ofMillis(3L)).grace(ofMillis(0));

        try {
            SessionWindows.with(ofMillis(3L)).grace(ofMillis(-1L));
            fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeNegative() {
        SessionWindows.with(ofMillis(-1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeZero() {
        SessionWindows.with(ofMillis(0));
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated apis
    @Test
    public void retentionTimeShouldBeGapIfGapIsLargerThanDefaultRetentionTime() {
        final long windowGap = 2 * SessionWindows.with(ofMillis(1)).maintainMs();
        assertEquals(windowGap, SessionWindows.with(ofMillis(windowGap)).maintainMs());
    }

    @Deprecated
    @Test
    public void retentionTimeMustNotBeNegative() {
        final SessionWindows windowSpec = SessionWindows.with(ofMillis(42));
        try {
            windowSpec.until(41);
            fail("should not accept retention time smaller than gap");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(SessionWindows.with(ofMillis(1)), SessionWindows.with(ofMillis(1)));

        verifyEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(6)), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)));

        verifyEquality(SessionWindows.with(ofMillis(1)).until(7), SessionWindows.with(ofMillis(1)).until(7));

        verifyEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(6)).until(7), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)).until(7));
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {
        verifyInEquality(SessionWindows.with(ofMillis(9)), SessionWindows.with(ofMillis(1)));

        verifyInEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(9)), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)));

        verifyInEquality(SessionWindows.with(ofMillis(1)).until(9), SessionWindows.with(ofMillis(1)).until(7));

        verifyInEquality(SessionWindows.with(ofMillis(2)).grace(ofMillis(6)).until(7), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)).until(7));

        verifyInEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(0)).until(7), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)).until(7));

        verifyInEquality(SessionWindows.with(ofMillis(1)).grace(ofMillis(6)).until(70), SessionWindows.with(ofMillis(1)).grace(ofMillis(6)).until(7));
    }
}
