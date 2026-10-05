/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */
package solutions.a2.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Decimal rounding for reported metric values.
 * <p>
 * Replaces {@code org.apache.commons.math3.util.Precision.round(..)}: commons-math3 is a 2.1 MiB
 * dependency of which only that one method was ever used.
 * <p>
 * Both overloads round HALF_UP on the shortest decimal representation of the argument, matching
 * commons-math3's {@code double} implementation exactly. The {@code float} overload does
 * <em>not</em> match commons-math3 bit for bit: commons scales by {@code 10^scale} in {@code float}
 * arithmetic, which loses precision above 2^24 and skews the final digit at magnitudes above
 * roughly 1e4. This implementation rounds the exact decimal value instead, so it agrees with
 * {@link BigDecimal} where commons-math3 does not.
 * <p>
 * NaN and infinities are returned unchanged, and the sign of a negative zero is preserved.
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class MathUtils {

	private MathUtils() {}

	/**
	 * Rounds {@code x} to {@code scale} digits after the decimal point, HALF_UP.
	 *
	 * @param x     value to round
	 * @param scale number of digits after the decimal point
	 * @return the rounded value; {@code x} itself when it is NaN or infinite
	 */
	public static double round(final double x, final int scale) {
		try {
			final double rounded = new BigDecimal(Double.toString(x))
					.setScale(scale, RoundingMode.HALF_UP)
					.doubleValue();
			return rounded == 0d ? 0d * x : rounded;
		} catch (NumberFormatException nfe) {
			return Double.isInfinite(x) ? x : Double.NaN;
		}
	}

	/**
	 * Rounds {@code x} to {@code scale} digits after the decimal point, HALF_UP.
	 *
	 * @param x     value to round
	 * @param scale number of digits after the decimal point
	 * @return the rounded value; {@code x} itself when it is NaN or infinite
	 */
	public static float round(final float x, final int scale) {
		try {
			final float rounded = new BigDecimal(Float.toString(x))
					.setScale(scale, RoundingMode.HALF_UP)
					.floatValue();
			return rounded == 0f ? 0f * x : rounded;
		} catch (NumberFormatException nfe) {
			return Float.isInfinite(x) ? x : Float.NaN;
		}
	}

}
