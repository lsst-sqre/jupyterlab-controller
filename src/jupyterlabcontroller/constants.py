"""Constants for jupyterlab-controller
"""

CONFIGURATION_PATH = "/etc/nublado/config.yaml"
DOCKER_SECRETS_PATH = "/etc/secrets/.dockerconfigjson"

ADMIN_SCOPE = "admin:jupyterlab"
USER_SCOPE = "exec:notebook"

KUBERNETES_REQUEST_TIMEOUT: int = 60

PREPULLER_POLL_INTERVAL: int = 60
PREPULLER_PULL_TIMEOUT: int = 600

SPAWNER_FORM_TEMPLATE = """
<script>
function selectDropdown() {
    document.getElementById('{{ dropdown_sentinel }}').checked = true;
}
</script>
<style>
    td {
        border: 1px solid black;
        padding: 2%;
        vertical-align: top;
    }
    .radio label,
    .checkbox label {
        padding-left: 0px;
    }
</style>
<table width="100%">
<tr>
  <th>Image</th>
  <th>Options</th>
</tr>
<tr>
<td width="50%">
  <div class="radio radio-inline">
{% for i in cached_images %}
    <input type="radio" name="image_list"
     id="image{{ loop.index }}" value="{{ i.path }}"
     {% if loop.first %} checked {% endif %}
    >
    <label for="image{{ loop.index }}">{{ i.name }}</label><br />
{% endfor %}
    <input type="radio" name="image_list"
        id="{{ dropdown_sentinel }}"
        value="{{ dropdown_sentinel }}"
        {% if not cached_images %} checked {% endif %}
    >
    <label for="{{ dropdown_sentinel }}">
      Select uncached image (slower start):
    </label><br />
    <select name="image_dropdown" onclick="selectDropdown()">
    {% for i in all_images %}
        <option value="{{ i.path }}">{{ i.name }}</option>
    {% endfor %}
    </select>
  </div>
</td>
<td width="50%">
  <div class="radio radio-inline">
{% for s in sizes %}
    <input type="radio" name="size"
     id="{{ s.name }}" value="{{ s.name }}"
     {% if loop.first %} checked {% endif %}
    >
    <label for="{{ s.name }}">
      {{ s.name }} ({{ s.cpu }} CPU, {{ s.memory }} RAM)
    </label><br />
{% endfor %}
  </div>
  <br />
  <br />
  <div class="checkbox checkbox-inline">
    <input type="checkbox" id="enable_debug"
     name="enable_debug" value="false">
    <label for="enable_debug">Enable debug logs</label><br />

    <input type="checkbox" id="reset_user_env"
     name="reset_user_env" value="false">
    <label for="reset_user_env">
      Reset user environment: relocate .cache, .jupyter, and .local
    </label><br />
  </div>
</td>
</tr>
</table>
"""