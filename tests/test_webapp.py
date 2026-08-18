#!/usr/bin/env python3
"""Экраны Mini App: список игр, урезанный набор внутри матча, «все игры».

    python3 tests/test_webapp.py

Приложение — один HTML на 2300 строк, и проверять его глазами дорого. Здесь
оно запускается в node с заглушкой DOM: начальные классы узлов берём ИЗ САМОЙ
РАЗМЕТКИ, иначе проверка врёт про состояние, которого в браузере не бывает
(так у меня и «провалился» поиск, который на деле работал).

Нет node — тест пропускается, а не падает: на сервере его может не быть.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP = ROOT / "docs" / "fantasy" / "index.html"

HARNESS = """
const INITIAL = %s;
function mkEl(id){ const o = { id, className:'', textContent:'', value:'',
  style:{}, children:[], onclick:null, disabled:false, _html:'',
  // innerHTML = '' в браузере выносит и детей. Без этого счётчик карточек
  // копится между отрисовками, и проверка врёт про число игр.
  get innerHTML(){ return this._html; },
  set innerHTML(v){ this._html = v; if (!v) this.children = []; },
  classList:{ _s:new Set(INITIAL[id] || []), add(c){this._s.add(c)}, remove(c){this._s.delete(c)},
    toggle(c,f){ f===undefined? (this._s.has(c)?this._s.delete(c):this._s.add(c)) : (f?this._s.add(c):this._s.delete(c)) },
    contains(c){return this._s.has(c)} },
  appendChild(x){ this.children.push(x) }, querySelector(){ return mkEl('q') },
  addEventListener(){}, focus(){}, dispatchEvent(){}, remove(){} }; return o; }
const _store = {};
global.document = { getElementById:(id)=> _store[id] || (_store[id]=mkEl(id)),
  createElement:(t)=> mkEl(t), querySelectorAll:()=>[],
  querySelector:(sel)=> _store['sel:'+sel] || (_store['sel:'+sel]=mkEl(sel)),
  addEventListener(){}, body: mkEl('body'), documentElement: mkEl('html') };
global.window = global;
global.location = { search:'', hash:'', href:'' };
global.navigator = { userAgent:'test' };
global.localStorage = { getItem:()=>null, setItem(){}, removeItem(){} };
global.fetch = async () => ({ ok:true, status:200, json: async()=>({}) });
global.Telegram = { WebApp: { ready(){}, expand(){}, initData:'', close(){},
  MainButton:{ setText(){}, show(){}, hide(){}, onClick(){}, showProgress(){}, hideProgress(){}, setParams(){} },
  HapticFeedback:{ impactOccurred(){}, notificationOccurred(){}, selectionChanged(){} },
  BackButton:{ show(){}, hide(){}, onClick(){} },
  themeParams:{}, colorScheme:'dark', onEvent(){}, sendData(){}, showPopup(){} } };
global.Event = class { constructor(t){ this.type=t } };
global.setTimeout = ()=>0; global.setInterval = ()=>0; global.clearInterval = ()=>{};
"""

CHECKS = """
  let bad = 0;
  const ok = (c, what) => { console.log((c?'  OK  ':'  FAIL')+' '+what); if(!c) bad++; };
  const byName = n => pool.find(p => p.name === n) || {};
  try {
    betGames = [
      {source:'infobasket', game_id:'777', date:'2026-08-22', time:'14:00',
       opponent:'Кирпичный Завод', label:'Кирпичный Завод · 22.08, 14:00'},
      {source:'slpro', game_id:'4600', date:'2026-08-24', time:'21:00',
       opponent:'Резалит', label:'Резалит · 24.08, 21:00'},
    ];
    renderGamesScreen();
    ok(document.getElementById('gamesCards').children.length === 3,
       'две игры плюс карточка «На все игры»');

    // Правила карточки «на всё» зависят от числа игр.
    betGames = [];
    renderGamesScreen();
    ok(document.getElementById('gamesCards').children.length === 1,
       'игр нет — одна карточка «На ближайшую игру»');
    ok(document.getElementById('gamesEmpty').classList.contains('hidden'),
       'и никакой пустой отговорки рядом');

    betGames = [{source:'infobasket', game_id:'777', date:'2026-08-22', time:'14:00',
                 opponent:'Кирпичный Завод', label:'x', declared:false}];
    renderGamesScreen();
    ok(document.getElementById('gamesCards').children.length === 1,
       'игра одна — лишней карточки нет');

    betGames = [
      {source:'infobasket', game_id:'777', date:'2026-08-22', time:'14:00',
       opponent:'Кирпичный Завод', label:'Кирпичный Завод · 22.08, 14:00', declared:true},
      {source:'slpro', game_id:'4600', date:'2026-08-24', time:'21:00',
       opponent:'Резалит', label:'Резалит · 24.08, 21:00', declared:false},
      {source:'slpro', game_id:'4601', date:'2026-08-29', time:'15:00',
       opponent:'BCC', label:'BCC · 29.08, 15:00', declared:false},
    ];
    renderGamesScreen();
    ok(document.getElementById('gamesCards').children.length === 4,
       'три объявленные игры видны, плюс «На все игры»');

    // Состава ещё нет — выбор из полного ростера, и это объяснено.
    applyGamePool({ game:{label:'Резалит · 24.08', date:'2026-08-24',
                            time:'21:00', opponent:'Резалит'}, declared:false,
      pool:[{ref:'ib:1:p1', name:'Иванов Иван'}, {ref:'ib:1:p2', name:'Петров Пётр'}],
      pick:[] }, 'slpro:4600');
    ok(pool.length === 2, 'без заявки показан весь ростер');
    ok(document.getElementById('gameWhen').textContent.indexOf('августа') >= 0,
       'дата матча написана словами: ' + document.getElementById('gameWhen').textContent);
    ok(!document.getElementById('poolNote').classList.contains('hidden'),
       'и человеку сказано, почему список полный');

    applyGamePool({ game:{label:'Кирпичный Завод · 22.08', date:'2026-08-22',
                            time:'14:00', opponent:'Кирпичный Завод'},
      pool:[{refs:['ib:1:p1'], name:'Иванов Иван', games:2, in_games:['a','b']},
            {refs:[], name:'Гость Гостев', unlinked:true}], pick:[] }, 'infobasket:777');
    ok(byName('Гость Гостев').unlinked === true, 'несведённый с лигой помечен');
    ok(byName('Иванов Иван').games === 2, 'играющий дважды отмечен счётчиком');
    // Инструменты выбора внутри матча ОСТАЮТСЯ: без фильтра по стоимости в
    // бюджетном режиме состав не собрать. Я их было убрал — стало хуже.
    ok(!document.getElementById('sort').classList.contains('hidden'), 'сортировка на месте');
    ok(!document.getElementById('playedChip').classList.contains('hidden'), '«только с играми» на месте');
    ok(minimalChrome === false, 'фильтры не подавляются');
    // А верхние вкладки внутри матча не нужны — человек занят одним делом.
    ok(document.querySelector('.tabs').classList.contains('hidden'), 'верхних вкладок нет');
    ok(document.getElementById('gameName').textContent.length > 0, 'соперник в шапке назван');
    ok(document.getElementById('gameEmblem').textContent.length > 0, 'эмблема на месте');

    // Название команды без уточнения турнира в скобках.
    applyGamePool({ game:{label:'x', date:'2026-08-24', time:'21:00',
                          opponent:'Резалит (Летний Кубок Дивизион C)'},
      declared:true, pool:[{refs:['ib:1:p1'], name:'Иванов Иван'}], pick:[] },
      'slpro:4600');
    ok(document.getElementById('gameName').textContent === 'Резалит',
       'в шапке только команда: ' + document.getElementById('gameName').textContent);
    ok(document.getElementById('gameWhen').textContent.indexOf('(') < 0,
       'и в строке даты скобок нет: ' + document.getElementById('gameWhen').textContent);

    // Плашка про перенесённый состав — только для недельного набора.
    inherited = true;
    applyGamePool({ game:{label:'x', date:'2026-08-24', opponent:'Резалит'},
      declared:true, pool:[{refs:['ib:1:p1'], name:'Иванов Иван'}], pick:[] },
      'slpro:4600');
    ok(inherited === false, 'внутри матча состав не считается перенесённым');

    // Вкладки должны исчезать ДО ответа сервера, а не после.
    document.querySelector('.tabs').classList.remove('hidden');
    trimChrome(true);
    ok(document.querySelector('.tabs').classList.contains('hidden'),
       'вкладки убираются сразу, не дожидаясь данных');

    weekRefs = ['ib:1:p1'];
    applyGamePool({ game:{label:'Все игры'}, all:true, differ:true,
      pool:[{ref:'ib:1:p1', name:'Иванов Иван', in_games:['22.08'], games:1},
            {ref:'ib:1:p9', name:'Петров Пётр', in_games:[], games:0, not_declared:true}],
      pick:[] }, 'all');
    ok(minimalChrome === false, 'на «всех играх» кнопки возвращаются');
    ok(!document.getElementById('sort').classList.contains('hidden'), 'сортировка вернулась');
    ok(counts['ib:1:p1'] === 1, 'недельный состав восстановлен, выбор не потерян');
    ok(byName('Петров Пётр').notDeclared === true, 'незаявленный помечен');
    ok(!document.getElementById('poolNote').classList.contains('hidden'),
       'про разные заявки человеку сказано');
    ok(document.getElementById('pool').innerHTML !== undefined, 'список отрисован');

    ok(document.getElementById('search').classList.contains('hidden'), 'поиск спрятан за кнопку');
    toggleSearch();
    ok(!document.getElementById('search').classList.contains('hidden'), 'по кнопке разворачивается');
    toggleSearch();
    ok(document.getElementById('search').classList.contains('hidden'), 'и сворачивается обратно');

    showGames();
    ok(!document.getElementById('gamesScreen').classList.contains('hidden'), 'возврат к списку игр');
    ok(!document.querySelector('.tabs').classList.contains('hidden'), 'снаружи матча вкладки вернулись');
    ok(document.getElementById('draft').classList.contains('hidden'), 'набор при этом скрыт');

    // Экран не должен оставаться пустым ни при каких обстоятельствах: раньше
    // он рисовался только внутри успешного запроса за играми, и не дошедший
    // запрос оставлял человека перед пустотой.
    betGames = [];
    renderGamesScreen();
    ok(document.getElementById('gamesCards').children.length >= 1,
       'без данных экран всё равно даёт карточку, а не пустоту');
  } catch (e) { console.log('  FAIL исключение: ' + e.message); bad++; }
  console.log(bad ? ('НЕ ПРОШЛО: ' + bad) : 'ЭКРАНЫ: ВСЁ ЗЕЛЁНОЕ');
  process.exit(bad ? 1 : 0);
"""


def main() -> int:
    node = shutil.which("node")
    if not node:
        print("node не установлен — экраны приложения не проверяю (это не провал)")
        return 0
    html = APP.read_text()
    js = re.findall(r"<script>(.*?)</script>", html, re.S)[0]

    # Начальные классы — из разметки: именно они решают, свёрнут ли поиск.
    initial = {}
    for m in re.finditer(r'<\w+[^>]*\bid="([\w-]+)"[^>]*>', html):
        cm = re.search(r'class="([^"]*)"', m.group(0))
        initial[m.group(1)] = cm.group(1).split() if cm else []

    body = js.rstrip()
    if not body.endswith("})();"):
        print("❌ приложение больше не обёрнуто в IIFE — проверку надо чинить")
        return 1
    body = body[: -len("})();")].replace("  init();", CHECKS)

    path = Path(tempfile.mkdtemp()) / "app.js"
    path.write_text(HARNESS % repr(initial).replace("'", '"') + body + "})();")
    r = subprocess.run([node, str(path)], capture_output=True, text=True)
    print(r.stdout.strip() or r.stderr[-800:])
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
