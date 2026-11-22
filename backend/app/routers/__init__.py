"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, cross_module

# 跨模块固定路径（/api/cross/...）必须先于通用资源路由注册，
# 否则会被 /api/{module}/{entry_id} 抢先匹配。
ROUTERS = [cross_module, resource]