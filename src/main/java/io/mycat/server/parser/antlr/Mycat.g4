/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

grammar Mycat;



mycat:
        msql
     |
        sql
     ;

msql :
       show_msql
    |
       clear_msql
    |
       kill_msql
    |
       reload_msql
    |
       rollback_msql
    |
       switch_msql
    |
       stop_msql
    |
       offline_msql
    |
       online_msql
    ;

sql:

    begin_sql
   |
    commit_sql
   |
    rollback_sql
   |
    delete_sql
   |
    insert_sql
   |
    replace_sql
   |
    select_sql
   |
    set_sql
   |
    show_sql
   |
    start_sql
   |
    update_sql
   |
    kill_sql
   |
    savepoint_sql
   |
    use_sql
   |
    explain_sql
   |
    kill_query_sql
   |
    help_sql
   |
     mysql_comment_sql //skip comment is empty
   |
    call_sql
   |
    describe_sql
   |
    load_data_infile_sql
   |
    ddl_sql
   ;


//mycat manager sql bein;
show_msql:
             SHOW MSQL_SYS_ID (WHERE ID EQN ID)?;

clear_msql:
             CLEAR MSQL_SYS_ID WHERE ID EQN (ID|NUM);

kill_msql:  KILL MSQL_SYS_ID ID(COMMA ID)*;

reload_msql:
            RELOAD MSQL_SYS_ID;

rollback_msql:
            ROLLBACK MSQL_SYS_ID;

switch_msql:
            SWITCH  MSQL_SYS_ID NAMESPACE_ID;

stop_msql: STOP MSQL_SYS_ID NAMESPACE_ID;

offline_msql:
          OFFLINE;

online_msql:
          ONLINE;



//mycat manager sql end;




//mysql sql begin;
show_sql: SHOW (ID | NAMESPACE_ID);

begin_sql: BEGIN;

commit_sql: COMMIT;

rollback_sql: ROLLBACK (ID|NAMESPACE_ID)?;

delete_sql: DELETE .*?;

insert_sql: INSERT .*?;

replace_sql: REPLACE .*?;

select_sql : SELECT .*?;

kill_sql: KILL (ID|NAMESPACE_ID);

set_sql : SET .*?;//TODO just find check


start_sql: START;

update_sql: UPDATE .*?;

savepoint_sql: SAVEPOINT (ID|NAMESPACE_ID);

use_sql : USE (ID|NAMESPACE_ID);

explain_sql: EXPLAIN .*?;

ddl_sql: (CREATE | DROP | TRUNCATE) .*?;

kill_query_sql: KILL QUERY (ID|NAMESPACE_ID);

help_sql : HELP .*?;

call_sql : CALL .*?;

describe_sql : (DESC|DESCRIBE) .*?;

load_data_infile_sql : LOAD DATA INFILE .*?;

mysql_comment_sql:;

//mysql sql end;



SET:

       ('s'|'S')('e'|'E')('t'|'T');
CLEAR:

         ('c'|'C')('l'|'L')('e'|'E')('a'|'A')('r'|'R');
WHERE:

         ('w'|'W')('h'|'H')('e'|'E')('r'|'R')('e'|'E');

SHOW
    :
    ('s'|'S')('h'|'H')('o'|'O')('w'|'W');


KILL:
    ('k'|'K')('I'|'i')('l'|'L')('l'|'L');


RELOAD :
     ('r'|'R')('e'|'E')('l'|'L')('o'|'O')('a'|'A')('d'|'D');

ROLLBACK:
     ('r'|'R')('o'|'O')('l'|'L')('l'|'L')('b'|'B')('a'|'A')('c'|'C')('k'|'K');

SWITCH:('s'|'S')('w'|'W')('i'|'I')('t'|'T')('c'|'C')('h'|'H');

STOP : ('s'|'S')('t'|'T')('o'|'O')('p'|'P');

OFFLINE: ('o'|'O')('f'|'F')('f'|'F')('l'|'L')('i'|'I')('n'|'N')('e'|'E');

ONLINE:('online'|'ONLINE');

BEGIN: ('b'|'B')('e'|'E')('g'|'G')('i'|'I')('n'|'N');

COMMIT: ('c'|'C')('o'|'O')('m'|'M')('m'|'M')('i'|'I')('t'|'T');

DELETE:('d'|'D')('e'|'E')('l'|'L')('e'|'E')('t'|'T')('e'|'E');

INSERT : ('i'|'I')('n'|'N')('s'|'S')('e'|'E')('r'|'R')('t'|'T');

REPLACE: ('r'|'R')('e'|'E')('p'|'P')('l'|'L')('a'|'A')('c'|'C')('e'|'E');

SELECT:('s'|'S')('e'|'E')('l'|'L')('e'|'E')('c'|'C')('t'|'T');

START: ('s'|'S')('t'|'T')('a'|'A')('r'|'R')('t'|'T');
UPDATE:('u'|'U')('p'|'P')('d'|'D')('a'|'A')('t'|'T')('e'|'E');

SAVEPOINT : ('s'|'S')('a'|'A')('v'|'V')('e'|'E')('p'|'P')('o'|'O')('i'|'I')('n'|'N')('t'|'T');

EXPLAIN : ('e'|'E')('x'|'X')('p'|'P')('l'|'L')('a'|'A')('i'|'I')('n'|'N');

USE: ('u'|'U')('s'|'S')('e'|'E');

CREATE : ('c'|'C')('r'|'R')('e'|'E')('a'|'A')('t'|'T')('e'|'E');

TRUNCATE : ('t'|'T')('r'|'R')('u'|'U')('n'|'N')('c'|'C')('a'|'A')('t'|'T')('e'|'E');

DROP :('d'|'D')('r'|'R')('o'|'O')('p'|'P');

QUERY : ('q'|'Q')('u'|'U')('e'|'E')('r'|'R')('y'|'Y');

HELP: (('?')|(('h'|'H')('e'|'E')('l'|'L')('p'|'P')));

CALL : ('c'|'C')('a'|'A')('l'|'L')('l'|'L');

DESC :('d'|'D')('e'|'E')('s'|'S')('c'|'C');

DESCRIBE: ('d'|'D')('e'|'E')('s'|'S')('c'|'C')('r'|'R')('i'|'I')('b'|'B')('e'|'E');

LOAD :('l'|'L')('o'|'O')('a'|'A')('d'|'D');

DATA:('d'|'D')('a'|'A')('t'|'T')('a'|'A');

INFILE: ('i'|'I')('n'|'N')('f'|'F')('i'|'I')('l'|'L')('e'|'E');




MSQL_SYS_ID :
             ('@@')(ID |NAMESPACE_ID);

NAMESPACE_ID : ID ((':'|'.')ID)+;

EQN: '=';
COMMA: ',';
DOT: '.';
ASTERISK: '*' ;
RPAREN	: ')' ;
LPAREN	: '(' ;
RBRACK	: ']' ;
LBRACK	: '[' ;
PLUS	: '+' ;
MINUS	: '-' ;
NEGATION: '~' ;
VERTBAR	: '|' ;
BITAND	: '&' ;
POWER_OP: '^' ;

ID     :('a'..'z'|'A'..'Z'|'_'|'\u4e00'..'\u9fa5') ('a'..'z'|'A'..'Z'|'_'|'0'..'9'|'\u4e00'..'\u9fa5')* ;


NUM    :'0'..'9'+  ('.' '0'..'9'+ )? ;

STRING : '\'' .*?  '\'';

COMMENT_LINE :
       (('--'|'#') ~('\n'|'\r')* ('\r'? '\n'|EOF) )-> skip;
COMMENT_PIECE:
        '/*' .*? '*/' -> skip;
WS     :(' '|'\t'|'\n'|'\r'|'\u3000')+ -> skip;
