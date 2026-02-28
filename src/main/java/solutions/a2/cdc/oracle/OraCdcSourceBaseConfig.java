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

package solutions.a2.cdc.oracle;

import java.util.List;
import java.util.Map;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface OraCdcSourceBaseConfig {

	public String rdbmsUrl();
	public String rdbmsUser();
	public String rdbmsPassword();
	public String walletLocation();
	public String kafkaTopic();
	public void kafkaTopic(final Map<String, String> taskParam);
	public int schemaType();
	public void schemaType(final Map<String, String> taskParam);
	public String topicOrPrefix();
	public List<String> includeObj();
	public List<String> excludeObj();
	public int pollIntervalMs();
	public void pollIntervalMs(final Map<String, String> taskParam);
	public int batchSize();

}
