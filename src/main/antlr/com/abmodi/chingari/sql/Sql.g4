grammar Sql;

singleStatement
    : statement ';'* EOF
    ;

statement
    : selectClause
      fromClause
      whereClause?
      aggregationClause?
    ;

selectClause
    : SELECT namedExpressionSeq
    ;

whereClause
    : WHERE booleanExpression
    ;

fromClause
    : FROM tableIdentifier
    ;

aggregationClause
    : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)*
    ;

tableIdentifier
    : table=IDENTIFIER
    ;

namedExpression
    : expression (AS? (name= IDENTIFIER))?
    ;

namedExpressionSeq
    : namedExpression (',' namedExpression)*
    ;

expression
    : booleanExpression
    | valueExpression
    | aggregateExpression
    ;

booleanExpression
    : NOT booleanExpression                                                         #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression                   #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                    #logicalBinary
    | left = valueExpression operator=comparisonOperator right = valueExpression    #compariosn
    ;

valueExpression
    : primaryExpression                                                             #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                       #arithmeticUnary
    | left = valueExpression operator=(ASTERISK | SLASH) right = valueExpression    #arithmeticBinary
    | left = valueExpression operator=(PLUS | MINUS) right = valueExpression        #arithmeticBinary
    ;

aggregateExpression
    : SUM '(' child = valueExpression ')'               #sumAggregate
    | MAX '(' child = valueExpression ')'               #maxAggregate
    | MIN '(' child = valueExpression ')'               #minAggregate
    | COUNT '(' child = valueExpression ')'             #countAggregate
    ;

primaryExpression
    : constant      #constantDefault
    | ASTERISK      #star
    | IDENTIFIER    #columnReference
    ;

constant
    : NULL                                                                                     #nullLiteral
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING+                                                                                  #stringLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

number
    : MINUS? INTEGER_VALUE #intLiteral
    | MINUS? LONG_VALUE #longLiteral
    | MINUS? DOUBLE_VALUE #doubleLiteral
    ;


SUM : 'SUM';
MAX : 'MAX';
MIN : 'MIN';
COUNT : 'COUNT';
SELECT : 'SELECT';
AS : 'AS';
BY : 'BY';
GROUP : 'GROUP';
FROM : 'FROM';
WHERE : 'WHERE';
NULL : 'NULL';
OR : 'OR';
AND : 'AND';
NOT : 'NOT';
TRUE : 'TRUE';
FALSE : 'FALSE';

EQ  : '=' | '==';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

LONG_VALUE
    : DIGIT+ 'L'
    ;

DOUBLE_VALUE
    : DECIMAL_DIGITS
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment LETTER
    : [A-Z]
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

UNRECOGNIZED
    : .
    ;