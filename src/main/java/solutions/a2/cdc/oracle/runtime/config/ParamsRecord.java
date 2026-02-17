/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package solutions.a2.cdc.oracle.runtime.config;

import java.util.List;
import java.util.Map;

record ParamsRecord(
		Map<String, String> numberMapParams,
		String incompleteDataTolerance,
		String topicNameStyle,
		String pkType,
		String lastProcessedSeqNotifier,
		List<String> keyOverride,
		String lobTransformClass,
		String tempDir,
		String startScn,
		String tableListStyle,
		int transactionsThreshold,
		String transactionImpl,
		String redoFileNameConversion,
		String medium,
		String sshProvider,
		String supplementalAll,
		String offHeapSize) {
}
