from __future__ import annotations

import os

from flask import Flask, jsonify

app = Flask(__name__)

AGENT_VERSION = '1.0.0'
_BASE = os.getenv('MASTERY_BASE', '/mnt/data')
_TEMPLATE = os.getenv('MASTERY_TEMPLATE', os.path.join(_BASE, 'CoE_Account_Mastery_Analysis_Templates.xlsm'))


@app.get('/')
def healthcheck():
    template_ok = os.path.isfile(_TEMPLATE)
    status = 'ok' if template_ok else 'degraded'
    return jsonify({
        'status': status,
        'agent': 'account_mastery',
        'version': AGENT_VERSION,
        'template_reachable': template_ok,
        'template_path': _TEMPLATE,
    }), 200 if template_ok else 503


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
