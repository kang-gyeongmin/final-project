#!/usr/bin/env bash
set -euo pipefail

# 참조 인프라(ASAC-DE-bigkk/sample)에서는 dags/, dbt/가 git submodule이라
# 이 스크립트가 두 서브모듈을 origin/main으로 갱신했다.
#
# 이 프로젝트는 dags/, dbt/를 별도 저장소로 분리하지 않고 이 레포 안에 그대로
# 두기로 했기 때문에(브레인스토밍 결정), 서브모듈 갱신이 필요 없다.
# deploy.sh와의 구조적 호환을 위해 스크립트 자체는 남겨두되 아무 것도 하지 않는다.

echo "이 프로젝트는 dags/, dbt/를 git submodule로 쓰지 않습니다. 갱신할 게 없습니다."
