// Generated from java-escape by ANTLR 4.11.1
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class SqlBaseParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.11.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		ABSENT=18, ADD=19, ADMIN=20, AFTER=21, ALL=22, ALTER=23, ANALYZE=24, AND=25, 
		ANY=26, ARRAY=27, AS=28, ASC=29, AT=30, AUTHORIZATION=31, BERNOULLI=32, 
		BETWEEN=33, BOTH=34, BY=35, CALL=36, CASCADE=37, CASE=38, CAST=39, CATALOG=40, 
		CATALOGS=41, COLUMN=42, COLUMNS=43, COMMENT=44, COMMIT=45, COMMITTED=46, 
		CONDITIONAL=47, CONSTRAINT=48, COUNT=49, COPARTITION=50, CREATE=51, CROSS=52, 
		CUBE=53, CURRENT=54, CURRENT_CATALOG=55, CURRENT_DATE=56, CURRENT_PATH=57, 
		CURRENT_ROLE=58, CURRENT_SCHEMA=59, CURRENT_TIME=60, CURRENT_TIMESTAMP=61, 
		CURRENT_USER=62, DATA=63, DATE=64, DAY=65, DEALLOCATE=66, DEFAULT=67, 
		DEFINE=68, DEFINER=69, DELETE=70, DENY=71, DESC=72, DESCRIBE=73, DESCRIPTOR=74, 
		DISTINCT=75, DISTRIBUTED=76, DOUBLE=77, DROP=78, ELSE=79, EMPTY=80, ENCODING=81, 
		END=82, ERROR=83, ESCAPE=84, EXCEPT=85, EXCLUDING=86, EXECUTE=87, EXISTS=88, 
		EXPLAIN=89, EXTRACT=90, FALSE=91, FETCH=92, FILTER=93, FINAL=94, FIRST=95, 
		FOLLOWING=96, FOR=97, FORMAT=98, FROM=99, FULL=100, FUNCTIONS=101, GRANT=102, 
		GRANTED=103, GRANTS=104, GRAPHVIZ=105, GROUP=106, GROUPING=107, GROUPS=108, 
		HAVING=109, HOUR=110, IF=111, IGNORE=112, IN=113, INCLUDING=114, INITIAL=115, 
		INNER=116, INPUT=117, INSERT=118, INTERSECT=119, INTERVAL=120, INTO=121, 
		INVOKER=122, IO=123, IS=124, ISOLATION=125, JOIN=126, JSON=127, JSON_ARRAY=128, 
		JSON_EXISTS=129, JSON_OBJECT=130, JSON_QUERY=131, JSON_VALUE=132, KEEP=133, 
		KEY=134, KEYS=135, LAST=136, LATERAL=137, LEADING=138, LEFT=139, LEVEL=140, 
		LIKE=141, LIMIT=142, LISTAGG=143, LOCAL=144, LOCALTIME=145, LOCALTIMESTAMP=146, 
		LOGICAL=147, MAP=148, MATCH=149, MATCHED=150, MATCHES=151, MATCH_RECOGNIZE=152, 
		MATERIALIZED=153, MEASURES=154, MERGE=155, MINUTE=156, MONTH=157, NATURAL=158, 
		NEXT=159, NFC=160, NFD=161, NFKC=162, NFKD=163, NO=164, NONE=165, NORMALIZE=166, 
		NOT=167, NULL=168, NULLIF=169, NULLS=170, OBJECT=171, OF=172, OFFSET=173, 
		OMIT=174, ON=175, ONE=176, ONLY=177, OPTION=178, OR=179, ORDER=180, ORDINALITY=181, 
		OUTER=182, OUTPUT=183, OVER=184, OVERFLOW=185, PARTITION=186, PARTITIONS=187, 
		PASSING=188, PAST=189, PATH=190, PATTERN=191, PER=192, PERMUTE=193, POSITION=194, 
		PRECEDING=195, PRECISION=196, PREPARE=197, PRIVILEGES=198, PROPERTIES=199, 
		PRUNE=200, QUOTES=201, RANGE=202, READ=203, RECURSIVE=204, REFRESH=205, 
		RENAME=206, REPEATABLE=207, REPLACE=208, RESET=209, RESPECT=210, RESTRICT=211, 
		RETURNING=212, REVOKE=213, RIGHT=214, ROLE=215, ROLES=216, ROLLBACK=217, 
		ROLLUP=218, ROW=219, ROWS=220, RUNNING=221, SCALAR=222, SCHEMA=223, SCHEMAS=224, 
		SECOND=225, SECURITY=226, SEEK=227, SELECT=228, SERIALIZABLE=229, SESSION=230, 
		SET=231, SETS=232, SHOW=233, SOME=234, START=235, STATS=236, SUBSET=237, 
		SUBSTRING=238, SYSTEM=239, TABLE=240, TABLES=241, TABLESAMPLE=242, TEXT=243, 
		TEXT_STRING=244, THEN=245, TIES=246, TIME=247, TIMESTAMP=248, TO=249, 
		TRAILING=250, TRANSACTION=251, TRIM=252, TRUE=253, TRUNCATE=254, TRY_CAST=255, 
		TYPE=256, UESCAPE=257, UNBOUNDED=258, UNCOMMITTED=259, UNCONDITIONAL=260, 
		UNION=261, UNIQUE=262, UNKNOWN=263, UNMATCHED=264, UNNEST=265, UPDATE=266, 
		USE=267, USER=268, USING=269, UTF16=270, UTF32=271, UTF8=272, VALIDATE=273, 
		VALUE=274, VALUES=275, VERBOSE=276, VERSION=277, VIEW=278, WHEN=279, WHERE=280, 
		WINDOW=281, WITH=282, WITHIN=283, WITHOUT=284, WORK=285, WRAPPER=286, 
		WRITE=287, YEAR=288, ZONE=289, EQ=290, NEQ=291, LT=292, LTE=293, GT=294, 
		GTE=295, PLUS=296, MINUS=297, ASTERISK=298, SLASH=299, PERCENT=300, CONCAT=301, 
		QUESTION_MARK=302, STRING=303, UNICODE_STRING=304, BINARY_LITERAL=305, 
		INTEGER_VALUE=306, DECIMAL_VALUE=307, DOUBLE_VALUE=308, IDENTIFIER=309, 
		DIGIT_IDENTIFIER=310, QUOTED_IDENTIFIER=311, BACKQUOTED_IDENTIFIER=312, 
		SIMPLE_COMMENT=313, BRACKETED_COMMENT=314, WS=315, UNRECOGNIZED=316, DELIMITER=317;
	public static final int
		RULE_singleStatement = 0, RULE_standaloneExpression = 1, RULE_standalonePathSpecification = 2, 
		RULE_standaloneType = 3, RULE_standaloneRowPattern = 4, RULE_statement = 5, 
		RULE_query = 6, RULE_with = 7, RULE_tableElement = 8, RULE_columnDefinition = 9, 
		RULE_likeClause = 10, RULE_properties = 11, RULE_propertyAssignments = 12, 
		RULE_property = 13, RULE_propertyValue = 14, RULE_catalogProperties = 15, 
		RULE_catalogPropertyAssignments = 16, RULE_catalogProperty = 17, RULE_queryNoWith = 18, 
		RULE_limitRowCount = 19, RULE_rowCount = 20, RULE_queryTerm = 21, RULE_queryPrimary = 22, 
		RULE_sortItem = 23, RULE_querySpecification = 24, RULE_groupBy = 25, RULE_groupingElement = 26, 
		RULE_groupingSet = 27, RULE_windowDefinition = 28, RULE_windowSpecification = 29, 
		RULE_namedQuery = 30, RULE_setQuantifier = 31, RULE_selectItem = 32, RULE_relation = 33, 
		RULE_joinType = 34, RULE_joinCriteria = 35, RULE_sampledRelation = 36, 
		RULE_sampleType = 37, RULE_trimsSpecification = 38, RULE_listAggOverflowBehavior = 39, 
		RULE_listaggCountIndication = 40, RULE_patternRecognition = 41, RULE_measureDefinition = 42, 
		RULE_rowsPerMatch = 43, RULE_emptyMatchHandling = 44, RULE_skipTo = 45, 
		RULE_subsetDefinition = 46, RULE_variableDefinition = 47, RULE_aliasedRelation = 48, 
		RULE_columnAliases = 49, RULE_relationPrimary = 50, RULE_tableFunctionCall = 51, 
		RULE_tableFunctionArgument = 52, RULE_tableArgument = 53, RULE_tableArgumentRelation = 54, 
		RULE_descriptorArgument = 55, RULE_descriptorField = 56, RULE_copartitionTables = 57, 
		RULE_expression = 58, RULE_booleanExpression = 59, RULE_predicate = 60, 
		RULE_valueExpression = 61, RULE_primaryExpression = 62, RULE_jsonPathInvocation = 63, 
		RULE_jsonValueExpression = 64, RULE_jsonRepresentation = 65, RULE_jsonArgument = 66, 
		RULE_jsonExistsErrorBehavior = 67, RULE_jsonValueBehavior = 68, RULE_jsonQueryWrapperBehavior = 69, 
		RULE_jsonQueryBehavior = 70, RULE_jsonObjectMember = 71, RULE_processingMode = 72, 
		RULE_nullTreatment = 73, RULE_string = 74, RULE_timeZoneSpecifier = 75, 
		RULE_comparisonOperator = 76, RULE_comparisonQuantifier = 77, RULE_booleanValue = 78, 
		RULE_interval = 79, RULE_intervalField = 80, RULE_normalForm = 81, RULE_type = 82, 
		RULE_rowField = 83, RULE_typeParameter = 84, RULE_whenClause = 85, RULE_filter = 86, 
		RULE_mergeCase = 87, RULE_over = 88, RULE_windowFrame = 89, RULE_frameExtent = 90, 
		RULE_frameBound = 91, RULE_rowPattern = 92, RULE_patternPrimary = 93, 
		RULE_patternQuantifier = 94, RULE_updateAssignment = 95, RULE_explainOption = 96, 
		RULE_transactionMode = 97, RULE_levelOfIsolation = 98, RULE_callArgument = 99, 
		RULE_pathElement = 100, RULE_pathSpecification = 101, RULE_privilege = 102, 
		RULE_qualifiedName = 103, RULE_queryPeriod = 104, RULE_rangeType = 105, 
		RULE_grantor = 106, RULE_principal = 107, RULE_roles = 108, RULE_identifier = 109, 
		RULE_number = 110, RULE_nonReserved = 111;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "standaloneExpression", "standalonePathSpecification", 
			"standaloneType", "standaloneRowPattern", "statement", "query", "with", 
			"tableElement", "columnDefinition", "likeClause", "properties", "propertyAssignments", 
			"property", "propertyValue", "catalogProperties", "catalogPropertyAssignments", 
			"catalogProperty", "queryNoWith", "limitRowCount", "rowCount", "queryTerm", 
			"queryPrimary", "sortItem", "querySpecification", "groupBy", "groupingElement", 
			"groupingSet", "windowDefinition", "windowSpecification", "namedQuery", 
			"setQuantifier", "selectItem", "relation", "joinType", "joinCriteria", 
			"sampledRelation", "sampleType", "trimsSpecification", "listAggOverflowBehavior", 
			"listaggCountIndication", "patternRecognition", "measureDefinition", 
			"rowsPerMatch", "emptyMatchHandling", "skipTo", "subsetDefinition", "variableDefinition", 
			"aliasedRelation", "columnAliases", "relationPrimary", "tableFunctionCall", 
			"tableFunctionArgument", "tableArgument", "tableArgumentRelation", "descriptorArgument", 
			"descriptorField", "copartitionTables", "expression", "booleanExpression", 
			"predicate", "valueExpression", "primaryExpression", "jsonPathInvocation", 
			"jsonValueExpression", "jsonRepresentation", "jsonArgument", "jsonExistsErrorBehavior", 
			"jsonValueBehavior", "jsonQueryWrapperBehavior", "jsonQueryBehavior", 
			"jsonObjectMember", "processingMode", "nullTreatment", "string", "timeZoneSpecifier", 
			"comparisonOperator", "comparisonQuantifier", "booleanValue", "interval", 
			"intervalField", "normalForm", "type", "rowField", "typeParameter", "whenClause", 
			"filter", "mergeCase", "over", "windowFrame", "frameExtent", "frameBound", 
			"rowPattern", "patternPrimary", "patternQuantifier", "updateAssignment", 
			"explainOption", "transactionMode", "levelOfIsolation", "callArgument", 
			"pathElement", "pathSpecification", "privilege", "qualifiedName", "queryPeriod", 
			"rangeType", "grantor", "principal", "roles", "identifier", "number", 
			"nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'.'", "'('", "')'", "','", "'SKIP'", "'=>'", "'->'", "'['", "']'", 
			"':'", "'|'", "'^'", "'$'", "'{-'", "'-}'", "'{'", "'}'", "'ABSENT'", 
			"'ADD'", "'ADMIN'", "'AFTER'", "'ALL'", "'ALTER'", "'ANALYZE'", "'AND'", 
			"'ANY'", "'ARRAY'", "'AS'", "'ASC'", "'AT'", "'AUTHORIZATION'", "'BERNOULLI'", 
			"'BETWEEN'", "'BOTH'", "'BY'", "'CALL'", "'CASCADE'", "'CASE'", "'CAST'", 
			"'CATALOG'", "'CATALOGS'", "'COLUMN'", "'COLUMNS'", "'COMMENT'", "'COMMIT'", 
			"'COMMITTED'", "'CONDITIONAL'", "'CONSTRAINT'", "'COUNT'", "'COPARTITION'", 
			"'CREATE'", "'CROSS'", "'CUBE'", "'CURRENT'", "'CURRENT_CATALOG'", "'CURRENT_DATE'", 
			"'CURRENT_PATH'", "'CURRENT_ROLE'", "'CURRENT_SCHEMA'", "'CURRENT_TIME'", 
			"'CURRENT_TIMESTAMP'", "'CURRENT_USER'", "'DATA'", "'DATE'", "'DAY'", 
			"'DEALLOCATE'", "'DEFAULT'", "'DEFINE'", "'DEFINER'", "'DELETE'", "'DENY'", 
			"'DESC'", "'DESCRIBE'", "'DESCRIPTOR'", "'DISTINCT'", "'DISTRIBUTED'", 
			"'DOUBLE'", "'DROP'", "'ELSE'", "'EMPTY'", "'ENCODING'", "'END'", "'ERROR'", 
			"'ESCAPE'", "'EXCEPT'", "'EXCLUDING'", "'EXECUTE'", "'EXISTS'", "'EXPLAIN'", 
			"'EXTRACT'", "'FALSE'", "'FETCH'", "'FILTER'", "'FINAL'", "'FIRST'", 
			"'FOLLOWING'", "'FOR'", "'FORMAT'", "'FROM'", "'FULL'", "'FUNCTIONS'", 
			"'GRANT'", "'GRANTED'", "'GRANTS'", "'GRAPHVIZ'", "'GROUP'", "'GROUPING'", 
			"'GROUPS'", "'HAVING'", "'HOUR'", "'IF'", "'IGNORE'", "'IN'", "'INCLUDING'", 
			"'INITIAL'", "'INNER'", "'INPUT'", "'INSERT'", "'INTERSECT'", "'INTERVAL'", 
			"'INTO'", "'INVOKER'", "'IO'", "'IS'", "'ISOLATION'", "'JOIN'", "'JSON'", 
			"'JSON_ARRAY'", "'JSON_EXISTS'", "'JSON_OBJECT'", "'JSON_QUERY'", "'JSON_VALUE'", 
			"'KEEP'", "'KEY'", "'KEYS'", "'LAST'", "'LATERAL'", "'LEADING'", "'LEFT'", 
			"'LEVEL'", "'LIKE'", "'LIMIT'", "'LISTAGG'", "'LOCAL'", "'LOCALTIME'", 
			"'LOCALTIMESTAMP'", "'LOGICAL'", "'MAP'", "'MATCH'", "'MATCHED'", "'MATCHES'", 
			"'MATCH_RECOGNIZE'", "'MATERIALIZED'", "'MEASURES'", "'MERGE'", "'MINUTE'", 
			"'MONTH'", "'NATURAL'", "'NEXT'", "'NFC'", "'NFD'", "'NFKC'", "'NFKD'", 
			"'NO'", "'NONE'", "'NORMALIZE'", "'NOT'", "'NULL'", "'NULLIF'", "'NULLS'", 
			"'OBJECT'", "'OF'", "'OFFSET'", "'OMIT'", "'ON'", "'ONE'", "'ONLY'", 
			"'OPTION'", "'OR'", "'ORDER'", "'ORDINALITY'", "'OUTER'", "'OUTPUT'", 
			"'OVER'", "'OVERFLOW'", "'PARTITION'", "'PARTITIONS'", "'PASSING'", "'PAST'", 
			"'PATH'", "'PATTERN'", "'PER'", "'PERMUTE'", "'POSITION'", "'PRECEDING'", 
			"'PRECISION'", "'PREPARE'", "'PRIVILEGES'", "'PROPERTIES'", "'PRUNE'", 
			"'QUOTES'", "'RANGE'", "'READ'", "'RECURSIVE'", "'REFRESH'", "'RENAME'", 
			"'REPEATABLE'", "'REPLACE'", "'RESET'", "'RESPECT'", "'RESTRICT'", "'RETURNING'", 
			"'REVOKE'", "'RIGHT'", "'ROLE'", "'ROLES'", "'ROLLBACK'", "'ROLLUP'", 
			"'ROW'", "'ROWS'", "'RUNNING'", "'SCALAR'", "'SCHEMA'", "'SCHEMAS'", 
			"'SECOND'", "'SECURITY'", "'SEEK'", "'SELECT'", "'SERIALIZABLE'", "'SESSION'", 
			"'SET'", "'SETS'", "'SHOW'", "'SOME'", "'START'", "'STATS'", "'SUBSET'", 
			"'SUBSTRING'", "'SYSTEM'", "'TABLE'", "'TABLES'", "'TABLESAMPLE'", "'TEXT'", 
			"'STRING'", "'THEN'", "'TIES'", "'TIME'", "'TIMESTAMP'", "'TO'", "'TRAILING'", 
			"'TRANSACTION'", "'TRIM'", "'TRUE'", "'TRUNCATE'", "'TRY_CAST'", "'TYPE'", 
			"'UESCAPE'", "'UNBOUNDED'", "'UNCOMMITTED'", "'UNCONDITIONAL'", "'UNION'", 
			"'UNIQUE'", "'UNKNOWN'", "'UNMATCHED'", "'UNNEST'", "'UPDATE'", "'USE'", 
			"'USER'", "'USING'", "'UTF16'", "'UTF32'", "'UTF8'", "'VALIDATE'", "'VALUE'", 
			"'VALUES'", "'VERBOSE'", "'VERSION'", "'VIEW'", "'WHEN'", "'WHERE'", 
			"'WINDOW'", "'WITH'", "'WITHIN'", "'WITHOUT'", "'WORK'", "'WRAPPER'", 
			"'WRITE'", "'YEAR'", "'ZONE'", "'='", null, "'<'", "'<='", "'>'", "'>='", 
			"'+'", "'-'", "'*'", "'/'", "'%'", "'||'", "'?'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, "ABSENT", "ADD", "ADMIN", "AFTER", 
			"ALL", "ALTER", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "AT", 
			"AUTHORIZATION", "BERNOULLI", "BETWEEN", "BOTH", "BY", "CALL", "CASCADE", 
			"CASE", "CAST", "CATALOG", "CATALOGS", "COLUMN", "COLUMNS", "COMMENT", 
			"COMMIT", "COMMITTED", "CONDITIONAL", "CONSTRAINT", "COUNT", "COPARTITION", 
			"CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE", 
			"CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", 
			"CURRENT_USER", "DATA", "DATE", "DAY", "DEALLOCATE", "DEFAULT", "DEFINE", 
			"DEFINER", "DELETE", "DENY", "DESC", "DESCRIBE", "DESCRIPTOR", "DISTINCT", 
			"DISTRIBUTED", "DOUBLE", "DROP", "ELSE", "EMPTY", "ENCODING", "END", 
			"ERROR", "ESCAPE", "EXCEPT", "EXCLUDING", "EXECUTE", "EXISTS", "EXPLAIN", 
			"EXTRACT", "FALSE", "FETCH", "FILTER", "FINAL", "FIRST", "FOLLOWING", 
			"FOR", "FORMAT", "FROM", "FULL", "FUNCTIONS", "GRANT", "GRANTED", "GRANTS", 
			"GRAPHVIZ", "GROUP", "GROUPING", "GROUPS", "HAVING", "HOUR", "IF", "IGNORE", 
			"IN", "INCLUDING", "INITIAL", "INNER", "INPUT", "INSERT", "INTERSECT", 
			"INTERVAL", "INTO", "INVOKER", "IO", "IS", "ISOLATION", "JOIN", "JSON", 
			"JSON_ARRAY", "JSON_EXISTS", "JSON_OBJECT", "JSON_QUERY", "JSON_VALUE", 
			"KEEP", "KEY", "KEYS", "LAST", "LATERAL", "LEADING", "LEFT", "LEVEL", 
			"LIKE", "LIMIT", "LISTAGG", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOGICAL", 
			"MAP", "MATCH", "MATCHED", "MATCHES", "MATCH_RECOGNIZE", "MATERIALIZED", 
			"MEASURES", "MERGE", "MINUTE", "MONTH", "NATURAL", "NEXT", "NFC", "NFD", 
			"NFKC", "NFKD", "NO", "NONE", "NORMALIZE", "NOT", "NULL", "NULLIF", "NULLS", 
			"OBJECT", "OF", "OFFSET", "OMIT", "ON", "ONE", "ONLY", "OPTION", "OR", 
			"ORDER", "ORDINALITY", "OUTER", "OUTPUT", "OVER", "OVERFLOW", "PARTITION", 
			"PARTITIONS", "PASSING", "PAST", "PATH", "PATTERN", "PER", "PERMUTE", 
			"POSITION", "PRECEDING", "PRECISION", "PREPARE", "PRIVILEGES", "PROPERTIES", 
			"PRUNE", "QUOTES", "RANGE", "READ", "RECURSIVE", "REFRESH", "RENAME", 
			"REPEATABLE", "REPLACE", "RESET", "RESPECT", "RESTRICT", "RETURNING", 
			"REVOKE", "RIGHT", "ROLE", "ROLES", "ROLLBACK", "ROLLUP", "ROW", "ROWS", 
			"RUNNING", "SCALAR", "SCHEMA", "SCHEMAS", "SECOND", "SECURITY", "SEEK", 
			"SELECT", "SERIALIZABLE", "SESSION", "SET", "SETS", "SHOW", "SOME", "START", 
			"STATS", "SUBSET", "SUBSTRING", "SYSTEM", "TABLE", "TABLES", "TABLESAMPLE", 
			"TEXT", "TEXT_STRING", "THEN", "TIES", "TIME", "TIMESTAMP", "TO", "TRAILING", 
			"TRANSACTION", "TRIM", "TRUE", "TRUNCATE", "TRY_CAST", "TYPE", "UESCAPE", 
			"UNBOUNDED", "UNCOMMITTED", "UNCONDITIONAL", "UNION", "UNIQUE", "UNKNOWN", 
			"UNMATCHED", "UNNEST", "UPDATE", "USE", "USER", "USING", "UTF16", "UTF32", 
			"UTF8", "VALIDATE", "VALUE", "VALUES", "VERBOSE", "VERSION", "VIEW", 
			"WHEN", "WHERE", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "WRAPPER", 
			"WRITE", "YEAR", "ZONE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", 
			"MINUS", "ASTERISK", "SLASH", "PERCENT", "CONCAT", "QUESTION_MARK", "STRING", 
			"UNICODE_STRING", "BINARY_LITERAL", "INTEGER_VALUE", "DECIMAL_VALUE", 
			"DOUBLE_VALUE", "IDENTIFIER", "DIGIT_IDENTIFIER", "QUOTED_IDENTIFIER", 
			"BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", 
			"UNRECOGNIZED", "DELIMITER"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "java-escape"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(224);
			statement();
			setState(225);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StandaloneExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandaloneExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandaloneExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandaloneExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandaloneExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandaloneExpressionContext standaloneExpression() throws RecognitionException {
		StandaloneExpressionContext _localctx = new StandaloneExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_standaloneExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(227);
			expression();
			setState(228);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StandalonePathSpecificationContext extends ParserRuleContext {
		public PathSpecificationContext pathSpecification() {
			return getRuleContext(PathSpecificationContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandalonePathSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standalonePathSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandalonePathSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandalonePathSpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandalonePathSpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandalonePathSpecificationContext standalonePathSpecification() throws RecognitionException {
		StandalonePathSpecificationContext _localctx = new StandalonePathSpecificationContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_standalonePathSpecification);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(230);
			pathSpecification();
			setState(231);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StandaloneTypeContext extends ParserRuleContext {
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandaloneTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandaloneType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandaloneType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandaloneType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandaloneTypeContext standaloneType() throws RecognitionException {
		StandaloneTypeContext _localctx = new StandaloneTypeContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_standaloneType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(233);
			type(0);
			setState(234);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StandaloneRowPatternContext extends ParserRuleContext {
		public RowPatternContext rowPattern() {
			return getRuleContext(RowPatternContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandaloneRowPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneRowPattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandaloneRowPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandaloneRowPattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandaloneRowPattern(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandaloneRowPatternContext standaloneRowPattern() throws RecognitionException {
		StandaloneRowPatternContext _localctx = new StandaloneRowPatternContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_standaloneRowPattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(236);
			rowPattern(0);
			setState(237);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public List<ExplainOptionContext> explainOption() {
			return getRuleContexts(ExplainOptionContext.class);
		}
		public ExplainOptionContext explainOption(int i) {
			return getRuleContext(ExplainOptionContext.class,i);
		}
		public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplain(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplain(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PrepareContext extends StatementContext {
		public TerminalNode PREPARE() { return getToken(SqlBaseParser.PREPARE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public PrepareContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrepare(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrepare(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrepare(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropMaterializedViewContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetMaterializedViewPropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public SetMaterializedViewPropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetMaterializedViewProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetMaterializedViewProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetMaterializedViewProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UseContext extends StatementContext {
		public IdentifierContext schema;
		public IdentifierContext catalog;
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public UseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUse(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DeallocateContext extends StatementContext {
		public TerminalNode DEALLOCATE() { return getToken(SqlBaseParser.DEALLOCATE, 0); }
		public TerminalNode PREPARE() { return getToken(SqlBaseParser.PREPARE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DeallocateContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDeallocate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDeallocate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDeallocate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameTableContext extends StatementContext {
		public QualifiedNameContext from;
		public QualifiedNameContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public RenameTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommitContext extends StatementContext {
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public CommitContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommit(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCommit(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateRoleContext extends StatementContext {
		public IdentifierContext name;
		public IdentifierContext catalog;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public GrantorContext grantor() {
			return getRuleContext(GrantorContext.class,0);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public CreateRoleContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateRole(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropColumnContext extends StatementContext {
		public QualifiedNameContext tableName;
		public QualifiedNameContext column;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public List<TerminalNode> IF() { return getTokens(SqlBaseParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(SqlBaseParser.IF, i);
		}
		public List<TerminalNode> EXISTS() { return getTokens(SqlBaseParser.EXISTS); }
		public TerminalNode EXISTS(int i) {
			return getToken(SqlBaseParser.EXISTS, i);
		}
		public DropColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropViewContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowTablesContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTables(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetViewAuthorizationContext extends StatementContext {
		public QualifiedNameContext from;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SetViewAuthorizationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetViewAuthorization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetViewAuthorization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetViewAuthorization(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCatalogsContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowCatalogsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCatalogs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCatalogs(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCatalogs(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowRolesContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public ShowRolesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowRoles(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MergeContext extends StatementContext {
		public TerminalNode MERGE() { return getToken(SqlBaseParser.MERGE, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<MergeCaseContext> mergeCase() {
			return getRuleContexts(MergeCaseContext.class);
		}
		public MergeCaseContext mergeCase(int i) {
			return getRuleContext(MergeCaseContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public MergeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMerge(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMerge(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMerge(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameColumnContext extends StatementContext {
		public QualifiedNameContext tableName;
		public IdentifierContext from;
		public IdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<TerminalNode> IF() { return getTokens(SqlBaseParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(SqlBaseParser.IF, i);
		}
		public List<TerminalNode> EXISTS() { return getTokens(SqlBaseParser.EXISTS); }
		public TerminalNode EXISTS(int i) {
			return getToken(SqlBaseParser.EXISTS, i);
		}
		public RenameColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommentColumnContext extends StatementContext {
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommentColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommentColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCommentColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RevokeRolesContext extends StatementContext {
		public IdentifierContext catalog;
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public RolesContext roles() {
			return getRuleContext(RolesContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<PrincipalContext> principal() {
			return getRuleContexts(PrincipalContext.class);
		}
		public PrincipalContext principal(int i) {
			return getRuleContext(PrincipalContext.class,i);
		}
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode GRANTED() { return getToken(SqlBaseParser.GRANTED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public GrantorContext grantor() {
			return getRuleContext(GrantorContext.class,0);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RevokeRolesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRevokeRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRevokeRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRevokeRoles(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateTableContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowColumnsContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowRoleGrantsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode GRANTS() { return getToken(SqlBaseParser.GRANTS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public ShowRoleGrantsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowRoleGrants(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowRoleGrants(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowRoleGrants(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AddColumnContext extends StatementContext {
		public QualifiedNameContext tableName;
		public ColumnDefinitionContext column;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ColumnDefinitionContext columnDefinition() {
			return getRuleContext(ColumnDefinitionContext.class,0);
		}
		public List<TerminalNode> IF() { return getTokens(SqlBaseParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(SqlBaseParser.IF, i);
		}
		public List<TerminalNode> EXISTS() { return getTokens(SqlBaseParser.EXISTS); }
		public TerminalNode EXISTS(int i) {
			return getToken(SqlBaseParser.EXISTS, i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public AddColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAddColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAddColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAddColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DenyContext extends StatementContext {
		public PrincipalContext grantee;
		public TerminalNode DENY() { return getToken(SqlBaseParser.DENY, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public List<PrivilegeContext> privilege() {
			return getRuleContexts(PrivilegeContext.class);
		}
		public PrivilegeContext privilege(int i) {
			return getRuleContext(PrivilegeContext.class,i);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public DenyContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDeny(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDeny(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDeny(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ResetSessionContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ResetSessionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResetSession(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResetSession(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitResetSession(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InsertIntoContext extends StatementContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public InsertIntoContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertInto(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertInto(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInsertInto(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowSessionContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowSessionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowSession(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowSession(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowSession(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateSchemaContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainAnalyzeContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode VERBOSE() { return getToken(SqlBaseParser.VERBOSE, 0); }
		public ExplainAnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplainAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplainAnalyze(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplainAnalyze(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExecuteContext extends StatementContext {
		public TerminalNode EXECUTE() { return getToken(SqlBaseParser.EXECUTE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ExecuteContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExecute(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExecute(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExecute(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameSchemaContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RenameSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropRoleContext extends StatementContext {
		public IdentifierContext name;
		public IdentifierContext catalog;
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public DropRoleContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropRole(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnalyzeContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public AnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnalyze(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAnalyze(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetRoleContext extends StatementContext {
		public IdentifierContext role;
		public IdentifierContext catalog;
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode NONE() { return getToken(SqlBaseParser.NONE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public SetRoleContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetRole(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowGrantsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode GRANTS() { return getToken(SqlBaseParser.GRANTS, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public ShowGrantsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowGrants(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowGrants(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowGrants(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropSchemaContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public DropSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTableAuthorizationContext extends StatementContext {
		public QualifiedNameContext tableName;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SetTableAuthorizationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableAuthorization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableAuthorization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableAuthorization(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateViewContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<TableElementContext> tableElement() {
			return getRuleContexts(TableElementContext.class);
		}
		public TableElementContext tableElement(int i) {
			return getRuleContext(TableElementContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StartTransactionContext extends StatementContext {
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public List<TransactionModeContext> transactionMode() {
			return getRuleContexts(TransactionModeContext.class);
		}
		public TransactionModeContext transactionMode(int i) {
			return getRuleContext(TransactionModeContext.class,i);
		}
		public StartTransactionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStartTransaction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStartTransaction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStartTransaction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableAsSelectContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public List<TerminalNode> WITH() { return getTokens(SqlBaseParser.WITH); }
		public TerminalNode WITH(int i) {
			return getToken(SqlBaseParser.WITH, i);
		}
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public CreateTableAsSelectContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableAsSelect(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableAsSelect(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTableAsSelect(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommentViewContext extends StatementContext {
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommentView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommentView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCommentView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowStatsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode STATS() { return getToken(SqlBaseParser.STATS, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowStatsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowStats(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowStats(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowStats(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateSchemaContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RevokeContext extends StatementContext {
		public PrincipalContext grantee;
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public List<PrivilegeContext> privilege() {
			return getRuleContexts(PrivilegeContext.class);
		}
		public PrivilegeContext privilege(int i) {
			return getRuleContext(PrivilegeContext.class,i);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public RevokeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRevoke(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRevoke(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRevoke(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UpdateContext extends StatementContext {
		public BooleanExpressionContext where;
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public List<UpdateAssignmentContext> updateAssignment() {
			return getRuleContexts(UpdateAssignmentContext.class);
		}
		public UpdateAssignmentContext updateAssignment(int i) {
			return getRuleContext(UpdateAssignmentContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public UpdateContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUpdate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUpdate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUpdate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableExecuteContext extends StatementContext {
		public QualifiedNameContext tableName;
		public IdentifierContext procedureName;
		public BooleanExpressionContext where;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode EXECUTE() { return getToken(SqlBaseParser.EXECUTE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public List<CallArgumentContext> callArgument() {
			return getRuleContexts(CallArgumentContext.class);
		}
		public CallArgumentContext callArgument(int i) {
			return getRuleContext(CallArgumentContext.class,i);
		}
		public TableExecuteContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableExecute(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableExecute(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableExecute(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DeleteContext extends StatementContext {
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public DeleteContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDelete(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDelete(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDelete(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeInputContext extends StatementContext {
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode INPUT() { return getToken(SqlBaseParser.INPUT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DescribeInputContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeInput(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeInput(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeInput(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowStatsForQueryContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode STATS() { return getToken(SqlBaseParser.STATS, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ShowStatsForQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowStatsForQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowStatsForQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowStatsForQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StatementDefaultContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStatementDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStatementDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTimeZoneContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SetTimeZoneContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTimeZone(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTimeZone(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTimeZone(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TruncateTableContext extends StatementContext {
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TruncateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTruncateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTruncateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTruncateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateMaterializedViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetSessionContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SetSessionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetSession(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetSession(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetSession(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode SECURITY() { return getToken(SqlBaseParser.SECURITY, 0); }
		public TerminalNode DEFINER() { return getToken(SqlBaseParser.DEFINER, 0); }
		public TerminalNode INVOKER() { return getToken(SqlBaseParser.INVOKER, 0); }
		public CreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameMaterializedViewContext extends StatementContext {
		public QualifiedNameContext from;
		public QualifiedNameContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public RenameMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowSchemasContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowSchemasContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowSchemas(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowSchemas(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowSchemas(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetSchemaAuthorizationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public SetSchemaAuthorizationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetSchemaAuthorization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetSchemaAuthorization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetSchemaAuthorization(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RollbackContext extends StatementContext {
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public RollbackContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRollback(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRollback(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRollback(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommentTableContext extends StatementContext {
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommentTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommentTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCommentTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameViewContext extends StatementContext {
		public QualifiedNameContext from;
		public QualifiedNameContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public RenameViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetPathContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode PATH() { return getToken(SqlBaseParser.PATH, 0); }
		public PathSpecificationContext pathSpecification() {
			return getRuleContext(PathSpecificationContext.class,0);
		}
		public SetPathContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetPath(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetPath(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetPath(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GrantRolesContext extends StatementContext {
		public IdentifierContext catalog;
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public RolesContext roles() {
			return getRuleContext(RolesContext.class,0);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<PrincipalContext> principal() {
			return getRuleContexts(PrincipalContext.class);
		}
		public PrincipalContext principal(int i) {
			return getRuleContext(PrincipalContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode GRANTED() { return getToken(SqlBaseParser.GRANTED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public GrantorContext grantor() {
			return getRuleContext(GrantorContext.class,0);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GrantRolesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGrantRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGrantRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGrantRoles(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CallContext extends StatementContext {
		public TerminalNode CALL() { return getToken(SqlBaseParser.CALL, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<CallArgumentContext> callArgument() {
			return getRuleContexts(CallArgumentContext.class);
		}
		public CallArgumentContext callArgument(int i) {
			return getRuleContext(CallArgumentContext.class,i);
		}
		public CallContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCall(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RefreshMaterializedViewContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public RefreshMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRefreshMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateMaterializedViewContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateCatalogContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public CatalogPropertiesContext catalogProperties() {
			return getRuleContext(CatalogPropertiesContext.class,0);
		}
		public CreateCatalogContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateCatalog(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateCatalog(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateCatalog(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowFunctionsContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowFunctions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowFunctions(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeOutputContext extends StatementContext {
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode OUTPUT() { return getToken(SqlBaseParser.OUTPUT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DescribeOutputContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeOutput(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeOutput(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeOutput(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GrantContext extends StatementContext {
		public PrincipalContext grantee;
		public List<TerminalNode> GRANT() { return getTokens(SqlBaseParser.GRANT); }
		public TerminalNode GRANT(int i) {
			return getToken(SqlBaseParser.GRANT, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public List<PrivilegeContext> privilege() {
			return getRuleContexts(PrivilegeContext.class);
		}
		public PrivilegeContext privilege(int i) {
			return getRuleContext(PrivilegeContext.class,i);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public GrantContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGrant(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGrant(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGrant(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTablePropertiesContext extends StatementContext {
		public QualifiedNameContext tableName;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_statement);
		int _la;
		try {
			setState(1028);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,108,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(239);
				query();
				}
				break;
			case 2:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(240);
				match(USE);
				setState(241);
				((UseContext)_localctx).schema = identifier();
				}
				break;
			case 3:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(242);
				match(USE);
				setState(243);
				((UseContext)_localctx).catalog = identifier();
				setState(244);
				match(T__0);
				setState(245);
				((UseContext)_localctx).schema = identifier();
				}
				break;
			case 4:
				_localctx = new CreateCatalogContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(247);
				match(CREATE);
				setState(248);
				match(CATALOG);
				setState(252);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(249);
					match(IF);
					setState(250);
					match(NOT);
					setState(251);
					match(EXISTS);
					}
					break;
				}
				setState(254);
				qualifiedName();
				setState(257);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(255);
					match(COMMENT);
					setState(256);
					string();
					}
				}

				setState(261);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(259);
					match(WITH);
					setState(260);
					catalogProperties();
					}
				}

				}
				break;
			case 5:
				_localctx = new CreateSchemaContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(263);
				match(CREATE);
				setState(264);
				match(SCHEMA);
				setState(268);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
				case 1:
					{
					setState(265);
					match(IF);
					setState(266);
					match(NOT);
					setState(267);
					match(EXISTS);
					}
					break;
				}
				setState(270);
				qualifiedName();
				setState(273);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AUTHORIZATION) {
					{
					setState(271);
					match(AUTHORIZATION);
					setState(272);
					principal();
					}
				}

				setState(277);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(275);
					match(WITH);
					setState(276);
					properties();
					}
				}

				}
				break;
			case 6:
				_localctx = new DropSchemaContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(279);
				match(DROP);
				setState(280);
				match(SCHEMA);
				setState(283);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
				case 1:
					{
					setState(281);
					match(IF);
					setState(282);
					match(EXISTS);
					}
					break;
				}
				setState(285);
				qualifiedName();
				setState(287);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(286);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case 7:
				_localctx = new RenameSchemaContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(289);
				match(ALTER);
				setState(290);
				match(SCHEMA);
				setState(291);
				qualifiedName();
				setState(292);
				match(RENAME);
				setState(293);
				match(TO);
				setState(294);
				identifier();
				}
				break;
			case 8:
				_localctx = new SetSchemaAuthorizationContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(296);
				match(ALTER);
				setState(297);
				match(SCHEMA);
				setState(298);
				qualifiedName();
				setState(299);
				match(SET);
				setState(300);
				match(AUTHORIZATION);
				setState(301);
				principal();
				}
				break;
			case 9:
				_localctx = new CreateTableAsSelectContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(303);
				match(CREATE);
				setState(304);
				match(TABLE);
				setState(308);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
				case 1:
					{
					setState(305);
					match(IF);
					setState(306);
					match(NOT);
					setState(307);
					match(EXISTS);
					}
					break;
				}
				setState(310);
				qualifiedName();
				setState(312);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(311);
					columnAliases();
					}
				}

				setState(316);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(314);
					match(COMMENT);
					setState(315);
					string();
					}
				}

				setState(320);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(318);
					match(WITH);
					setState(319);
					properties();
					}
				}

				setState(322);
				match(AS);
				setState(328);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
				case 1:
					{
					setState(323);
					query();
					}
					break;
				case 2:
					{
					setState(324);
					match(T__1);
					setState(325);
					query();
					setState(326);
					match(T__2);
					}
					break;
				}
				setState(335);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(330);
					match(WITH);
					setState(332);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==NO) {
						{
						setState(331);
						match(NO);
						}
					}

					setState(334);
					match(DATA);
					}
				}

				}
				break;
			case 10:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(337);
				match(CREATE);
				setState(338);
				match(TABLE);
				setState(342);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
				case 1:
					{
					setState(339);
					match(IF);
					setState(340);
					match(NOT);
					setState(341);
					match(EXISTS);
					}
					break;
				}
				setState(344);
				qualifiedName();
				setState(345);
				match(T__1);
				setState(346);
				tableElement();
				setState(351);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(347);
					match(T__3);
					setState(348);
					tableElement();
					}
					}
					setState(353);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(354);
				match(T__2);
				setState(357);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(355);
					match(COMMENT);
					setState(356);
					string();
					}
				}

				setState(361);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(359);
					match(WITH);
					setState(360);
					properties();
					}
				}

				}
				break;
			case 11:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(363);
				match(DROP);
				setState(364);
				match(TABLE);
				setState(367);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
				case 1:
					{
					setState(365);
					match(IF);
					setState(366);
					match(EXISTS);
					}
					break;
				}
				setState(369);
				qualifiedName();
				}
				break;
			case 12:
				_localctx = new InsertIntoContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(370);
				match(INSERT);
				setState(371);
				match(INTO);
				setState(372);
				qualifiedName();
				setState(374);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
				case 1:
					{
					setState(373);
					columnAliases();
					}
					break;
				}
				setState(376);
				query();
				}
				break;
			case 13:
				_localctx = new DeleteContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(378);
				match(DELETE);
				setState(379);
				match(FROM);
				setState(380);
				qualifiedName();
				setState(383);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(381);
					match(WHERE);
					setState(382);
					booleanExpression(0);
					}
				}

				}
				break;
			case 14:
				_localctx = new TruncateTableContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(385);
				match(TRUNCATE);
				setState(386);
				match(TABLE);
				setState(387);
				qualifiedName();
				}
				break;
			case 15:
				_localctx = new CommentTableContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(388);
				match(COMMENT);
				setState(389);
				match(ON);
				setState(390);
				match(TABLE);
				setState(391);
				qualifiedName();
				setState(392);
				match(IS);
				setState(395);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
				case UNICODE_STRING:
					{
					setState(393);
					string();
					}
					break;
				case NULL:
					{
					setState(394);
					match(NULL);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 16:
				_localctx = new CommentViewContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(397);
				match(COMMENT);
				setState(398);
				match(ON);
				setState(399);
				match(VIEW);
				setState(400);
				qualifiedName();
				setState(401);
				match(IS);
				setState(404);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
				case UNICODE_STRING:
					{
					setState(402);
					string();
					}
					break;
				case NULL:
					{
					setState(403);
					match(NULL);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 17:
				_localctx = new CommentColumnContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(406);
				match(COMMENT);
				setState(407);
				match(ON);
				setState(408);
				match(COLUMN);
				setState(409);
				qualifiedName();
				setState(410);
				match(IS);
				setState(413);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
				case UNICODE_STRING:
					{
					setState(411);
					string();
					}
					break;
				case NULL:
					{
					setState(412);
					match(NULL);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 18:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(415);
				match(ALTER);
				setState(416);
				match(TABLE);
				setState(419);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
				case 1:
					{
					setState(417);
					match(IF);
					setState(418);
					match(EXISTS);
					}
					break;
				}
				setState(421);
				((RenameTableContext)_localctx).from = qualifiedName();
				setState(422);
				match(RENAME);
				setState(423);
				match(TO);
				setState(424);
				((RenameTableContext)_localctx).to = qualifiedName();
				}
				break;
			case 19:
				_localctx = new AddColumnContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(426);
				match(ALTER);
				setState(427);
				match(TABLE);
				setState(430);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
				case 1:
					{
					setState(428);
					match(IF);
					setState(429);
					match(EXISTS);
					}
					break;
				}
				setState(432);
				((AddColumnContext)_localctx).tableName = qualifiedName();
				setState(433);
				match(ADD);
				setState(434);
				match(COLUMN);
				setState(438);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
				case 1:
					{
					setState(435);
					match(IF);
					setState(436);
					match(NOT);
					setState(437);
					match(EXISTS);
					}
					break;
				}
				setState(440);
				((AddColumnContext)_localctx).column = columnDefinition();
				}
				break;
			case 20:
				_localctx = new RenameColumnContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(442);
				match(ALTER);
				setState(443);
				match(TABLE);
				setState(446);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
				case 1:
					{
					setState(444);
					match(IF);
					setState(445);
					match(EXISTS);
					}
					break;
				}
				setState(448);
				((RenameColumnContext)_localctx).tableName = qualifiedName();
				setState(449);
				match(RENAME);
				setState(450);
				match(COLUMN);
				setState(453);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
				case 1:
					{
					setState(451);
					match(IF);
					setState(452);
					match(EXISTS);
					}
					break;
				}
				setState(455);
				((RenameColumnContext)_localctx).from = identifier();
				setState(456);
				match(TO);
				setState(457);
				((RenameColumnContext)_localctx).to = identifier();
				}
				break;
			case 21:
				_localctx = new DropColumnContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(459);
				match(ALTER);
				setState(460);
				match(TABLE);
				setState(463);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
				case 1:
					{
					setState(461);
					match(IF);
					setState(462);
					match(EXISTS);
					}
					break;
				}
				setState(465);
				((DropColumnContext)_localctx).tableName = qualifiedName();
				setState(466);
				match(DROP);
				setState(467);
				match(COLUMN);
				setState(470);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
				case 1:
					{
					setState(468);
					match(IF);
					setState(469);
					match(EXISTS);
					}
					break;
				}
				setState(472);
				((DropColumnContext)_localctx).column = qualifiedName();
				}
				break;
			case 22:
				_localctx = new SetTableAuthorizationContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(474);
				match(ALTER);
				setState(475);
				match(TABLE);
				setState(476);
				((SetTableAuthorizationContext)_localctx).tableName = qualifiedName();
				setState(477);
				match(SET);
				setState(478);
				match(AUTHORIZATION);
				setState(479);
				principal();
				}
				break;
			case 23:
				_localctx = new SetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(481);
				match(ALTER);
				setState(482);
				match(TABLE);
				setState(483);
				((SetTablePropertiesContext)_localctx).tableName = qualifiedName();
				setState(484);
				match(SET);
				setState(485);
				match(PROPERTIES);
				setState(486);
				propertyAssignments();
				}
				break;
			case 24:
				_localctx = new TableExecuteContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(488);
				match(ALTER);
				setState(489);
				match(TABLE);
				setState(490);
				((TableExecuteContext)_localctx).tableName = qualifiedName();
				setState(491);
				match(EXECUTE);
				setState(492);
				((TableExecuteContext)_localctx).procedureName = identifier();
				setState(505);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(493);
					match(T__1);
					setState(502);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (((_la) & ~0x3f) == 0 && ((1L << _la) & -304274493084532732L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967872150318926405L) != 0 || (((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -24910536512645121L) != 0 || (((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -9288743022235681L) != 0 || (((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 144048134953623005L) != 0) {
						{
						setState(494);
						callArgument();
						setState(499);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__3) {
							{
							{
							setState(495);
							match(T__3);
							setState(496);
							callArgument();
							}
							}
							setState(501);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(504);
					match(T__2);
					}
				}

				setState(509);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(507);
					match(WHERE);
					setState(508);
					((TableExecuteContext)_localctx).where = booleanExpression(0);
					}
				}

				}
				break;
			case 25:
				_localctx = new AnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(511);
				match(ANALYZE);
				setState(512);
				qualifiedName();
				setState(515);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(513);
					match(WITH);
					setState(514);
					properties();
					}
				}

				}
				break;
			case 26:
				_localctx = new CreateMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(517);
				match(CREATE);
				setState(520);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(518);
					match(OR);
					setState(519);
					match(REPLACE);
					}
				}

				setState(522);
				match(MATERIALIZED);
				setState(523);
				match(VIEW);
				setState(527);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
				case 1:
					{
					setState(524);
					match(IF);
					setState(525);
					match(NOT);
					setState(526);
					match(EXISTS);
					}
					break;
				}
				setState(529);
				qualifiedName();
				setState(532);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(530);
					match(COMMENT);
					setState(531);
					string();
					}
				}

				setState(536);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(534);
					match(WITH);
					setState(535);
					properties();
					}
				}

				setState(538);
				match(AS);
				setState(539);
				query();
				}
				break;
			case 27:
				_localctx = new CreateViewContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(541);
				match(CREATE);
				setState(544);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(542);
					match(OR);
					setState(543);
					match(REPLACE);
					}
				}

				setState(546);
				match(VIEW);
				setState(547);
				qualifiedName();
				setState(550);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(548);
					match(COMMENT);
					setState(549);
					string();
					}
				}

				setState(554);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SECURITY) {
					{
					setState(552);
					match(SECURITY);
					setState(553);
					_la = _input.LA(1);
					if ( !(_la==DEFINER || _la==INVOKER) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(556);
				match(AS);
				setState(557);
				query();
				}
				break;
			case 28:
				_localctx = new RefreshMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(559);
				match(REFRESH);
				setState(560);
				match(MATERIALIZED);
				setState(561);
				match(VIEW);
				setState(562);
				qualifiedName();
				}
				break;
			case 29:
				_localctx = new DropMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(563);
				match(DROP);
				setState(564);
				match(MATERIALIZED);
				setState(565);
				match(VIEW);
				setState(568);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
				case 1:
					{
					setState(566);
					match(IF);
					setState(567);
					match(EXISTS);
					}
					break;
				}
				setState(570);
				qualifiedName();
				}
				break;
			case 30:
				_localctx = new RenameMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(571);
				match(ALTER);
				setState(572);
				match(MATERIALIZED);
				setState(573);
				match(VIEW);
				setState(576);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
				case 1:
					{
					setState(574);
					match(IF);
					setState(575);
					match(EXISTS);
					}
					break;
				}
				setState(578);
				((RenameMaterializedViewContext)_localctx).from = qualifiedName();
				setState(579);
				match(RENAME);
				setState(580);
				match(TO);
				setState(581);
				((RenameMaterializedViewContext)_localctx).to = qualifiedName();
				}
				break;
			case 31:
				_localctx = new SetMaterializedViewPropertiesContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(583);
				match(ALTER);
				setState(584);
				match(MATERIALIZED);
				setState(585);
				match(VIEW);
				setState(586);
				qualifiedName();
				setState(587);
				match(SET);
				setState(588);
				match(PROPERTIES);
				setState(589);
				propertyAssignments();
				}
				break;
			case 32:
				_localctx = new DropViewContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(591);
				match(DROP);
				setState(592);
				match(VIEW);
				setState(595);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
				case 1:
					{
					setState(593);
					match(IF);
					setState(594);
					match(EXISTS);
					}
					break;
				}
				setState(597);
				qualifiedName();
				}
				break;
			case 33:
				_localctx = new RenameViewContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(598);
				match(ALTER);
				setState(599);
				match(VIEW);
				setState(600);
				((RenameViewContext)_localctx).from = qualifiedName();
				setState(601);
				match(RENAME);
				setState(602);
				match(TO);
				setState(603);
				((RenameViewContext)_localctx).to = qualifiedName();
				}
				break;
			case 34:
				_localctx = new SetViewAuthorizationContext(_localctx);
				enterOuterAlt(_localctx, 34);
				{
				setState(605);
				match(ALTER);
				setState(606);
				match(VIEW);
				setState(607);
				((SetViewAuthorizationContext)_localctx).from = qualifiedName();
				setState(608);
				match(SET);
				setState(609);
				match(AUTHORIZATION);
				setState(610);
				principal();
				}
				break;
			case 35:
				_localctx = new CallContext(_localctx);
				enterOuterAlt(_localctx, 35);
				{
				setState(612);
				match(CALL);
				setState(613);
				qualifiedName();
				setState(614);
				match(T__1);
				setState(623);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -304274493084532732L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967872150318926405L) != 0 || (((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -24910536512645121L) != 0 || (((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -9288743022235681L) != 0 || (((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 144048134953623005L) != 0) {
					{
					setState(615);
					callArgument();
					setState(620);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(616);
						match(T__3);
						setState(617);
						callArgument();
						}
						}
						setState(622);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(625);
				match(T__2);
				}
				break;
			case 36:
				_localctx = new CreateRoleContext(_localctx);
				enterOuterAlt(_localctx, 36);
				{
				setState(627);
				match(CREATE);
				setState(628);
				match(ROLE);
				setState(629);
				((CreateRoleContext)_localctx).name = identifier();
				setState(633);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(630);
					match(WITH);
					setState(631);
					match(ADMIN);
					setState(632);
					grantor();
					}
				}

				setState(637);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IN) {
					{
					setState(635);
					match(IN);
					setState(636);
					((CreateRoleContext)_localctx).catalog = identifier();
					}
				}

				}
				break;
			case 37:
				_localctx = new DropRoleContext(_localctx);
				enterOuterAlt(_localctx, 37);
				{
				setState(639);
				match(DROP);
				setState(640);
				match(ROLE);
				setState(641);
				((DropRoleContext)_localctx).name = identifier();
				setState(644);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IN) {
					{
					setState(642);
					match(IN);
					setState(643);
					((DropRoleContext)_localctx).catalog = identifier();
					}
				}

				}
				break;
			case 38:
				_localctx = new GrantRolesContext(_localctx);
				enterOuterAlt(_localctx, 38);
				{
				setState(646);
				match(GRANT);
				setState(647);
				roles();
				setState(648);
				match(TO);
				setState(649);
				principal();
				setState(654);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(650);
					match(T__3);
					setState(651);
					principal();
					}
					}
					setState(656);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(660);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(657);
					match(WITH);
					setState(658);
					match(ADMIN);
					setState(659);
					match(OPTION);
					}
				}

				setState(665);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GRANTED) {
					{
					setState(662);
					match(GRANTED);
					setState(663);
					match(BY);
					setState(664);
					grantor();
					}
				}

				setState(669);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IN) {
					{
					setState(667);
					match(IN);
					setState(668);
					((GrantRolesContext)_localctx).catalog = identifier();
					}
				}

				}
				break;
			case 39:
				_localctx = new RevokeRolesContext(_localctx);
				enterOuterAlt(_localctx, 39);
				{
				setState(671);
				match(REVOKE);
				setState(675);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
				case 1:
					{
					setState(672);
					match(ADMIN);
					setState(673);
					match(OPTION);
					setState(674);
					match(FOR);
					}
					break;
				}
				setState(677);
				roles();
				setState(678);
				match(FROM);
				setState(679);
				principal();
				setState(684);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(680);
					match(T__3);
					setState(681);
					principal();
					}
					}
					setState(686);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(690);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GRANTED) {
					{
					setState(687);
					match(GRANTED);
					setState(688);
					match(BY);
					setState(689);
					grantor();
					}
				}

				setState(694);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IN) {
					{
					setState(692);
					match(IN);
					setState(693);
					((RevokeRolesContext)_localctx).catalog = identifier();
					}
				}

				}
				break;
			case 40:
				_localctx = new SetRoleContext(_localctx);
				enterOuterAlt(_localctx, 40);
				{
				setState(696);
				match(SET);
				setState(697);
				match(ROLE);
				setState(701);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
				case 1:
					{
					setState(698);
					match(ALL);
					}
					break;
				case 2:
					{
					setState(699);
					match(NONE);
					}
					break;
				case 3:
					{
					setState(700);
					((SetRoleContext)_localctx).role = identifier();
					}
					break;
				}
				setState(705);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IN) {
					{
					setState(703);
					match(IN);
					setState(704);
					((SetRoleContext)_localctx).catalog = identifier();
					}
				}

				}
				break;
			case 41:
				_localctx = new GrantContext(_localctx);
				enterOuterAlt(_localctx, 41);
				{
				setState(707);
				match(GRANT);
				setState(718);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CREATE:
				case DELETE:
				case INSERT:
				case SELECT:
				case UPDATE:
					{
					setState(708);
					privilege();
					setState(713);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(709);
						match(T__3);
						setState(710);
						privilege();
						}
						}
						setState(715);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case ALL:
					{
					setState(716);
					match(ALL);
					setState(717);
					match(PRIVILEGES);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(720);
				match(ON);
				setState(722);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
				case 1:
					{
					setState(721);
					_la = _input.LA(1);
					if ( !(_la==SCHEMA || _la==TABLE) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(724);
				qualifiedName();
				setState(725);
				match(TO);
				setState(726);
				((GrantContext)_localctx).grantee = principal();
				setState(730);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(727);
					match(WITH);
					setState(728);
					match(GRANT);
					setState(729);
					match(OPTION);
					}
				}

				}
				break;
			case 42:
				_localctx = new DenyContext(_localctx);
				enterOuterAlt(_localctx, 42);
				{
				setState(732);
				match(DENY);
				setState(743);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CREATE:
				case DELETE:
				case INSERT:
				case SELECT:
				case UPDATE:
					{
					setState(733);
					privilege();
					setState(738);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(734);
						match(T__3);
						setState(735);
						privilege();
						}
						}
						setState(740);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case ALL:
					{
					setState(741);
					match(ALL);
					setState(742);
					match(PRIVILEGES);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(745);
				match(ON);
				setState(747);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
				case 1:
					{
					setState(746);
					_la = _input.LA(1);
					if ( !(_la==SCHEMA || _la==TABLE) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(749);
				qualifiedName();
				setState(750);
				match(TO);
				setState(751);
				((DenyContext)_localctx).grantee = principal();
				}
				break;
			case 43:
				_localctx = new RevokeContext(_localctx);
				enterOuterAlt(_localctx, 43);
				{
				setState(753);
				match(REVOKE);
				setState(757);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GRANT) {
					{
					setState(754);
					match(GRANT);
					setState(755);
					match(OPTION);
					setState(756);
					match(FOR);
					}
				}

				setState(769);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CREATE:
				case DELETE:
				case INSERT:
				case SELECT:
				case UPDATE:
					{
					setState(759);
					privilege();
					setState(764);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(760);
						match(T__3);
						setState(761);
						privilege();
						}
						}
						setState(766);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case ALL:
					{
					setState(767);
					match(ALL);
					setState(768);
					match(PRIVILEGES);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(771);
				match(ON);
				setState(773);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
				case 1:
					{
					setState(772);
					_la = _input.LA(1);
					if ( !(_la==SCHEMA || _la==TABLE) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(775);
				qualifiedName();
				setState(776);
				match(FROM);
				setState(777);
				((RevokeContext)_localctx).grantee = principal();
				}
				break;
			case 44:
				_localctx = new ShowGrantsContext(_localctx);
				enterOuterAlt(_localctx, 44);
				{
				setState(779);
				match(SHOW);
				setState(780);
				match(GRANTS);
				setState(786);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(781);
					match(ON);
					setState(783);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==TABLE) {
						{
						setState(782);
						match(TABLE);
						}
					}

					setState(785);
					qualifiedName();
					}
				}

				}
				break;
			case 45:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 45);
				{
				setState(788);
				match(EXPLAIN);
				setState(800);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
				case 1:
					{
					setState(789);
					match(T__1);
					setState(790);
					explainOption();
					setState(795);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(791);
						match(T__3);
						setState(792);
						explainOption();
						}
						}
						setState(797);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(798);
					match(T__2);
					}
					break;
				}
				setState(802);
				statement();
				}
				break;
			case 46:
				_localctx = new ExplainAnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 46);
				{
				setState(803);
				match(EXPLAIN);
				setState(804);
				match(ANALYZE);
				setState(806);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==VERBOSE) {
					{
					setState(805);
					match(VERBOSE);
					}
				}

				setState(808);
				statement();
				}
				break;
			case 47:
				_localctx = new ShowCreateTableContext(_localctx);
				enterOuterAlt(_localctx, 47);
				{
				setState(809);
				match(SHOW);
				setState(810);
				match(CREATE);
				setState(811);
				match(TABLE);
				setState(812);
				qualifiedName();
				}
				break;
			case 48:
				_localctx = new ShowCreateSchemaContext(_localctx);
				enterOuterAlt(_localctx, 48);
				{
				setState(813);
				match(SHOW);
				setState(814);
				match(CREATE);
				setState(815);
				match(SCHEMA);
				setState(816);
				qualifiedName();
				}
				break;
			case 49:
				_localctx = new ShowCreateViewContext(_localctx);
				enterOuterAlt(_localctx, 49);
				{
				setState(817);
				match(SHOW);
				setState(818);
				match(CREATE);
				setState(819);
				match(VIEW);
				setState(820);
				qualifiedName();
				}
				break;
			case 50:
				_localctx = new ShowCreateMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 50);
				{
				setState(821);
				match(SHOW);
				setState(822);
				match(CREATE);
				setState(823);
				match(MATERIALIZED);
				setState(824);
				match(VIEW);
				setState(825);
				qualifiedName();
				}
				break;
			case 51:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 51);
				{
				setState(826);
				match(SHOW);
				setState(827);
				match(TABLES);
				setState(830);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(828);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(829);
					qualifiedName();
					}
				}

				setState(838);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(832);
					match(LIKE);
					setState(833);
					((ShowTablesContext)_localctx).pattern = string();
					setState(836);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(834);
						match(ESCAPE);
						setState(835);
						((ShowTablesContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 52:
				_localctx = new ShowSchemasContext(_localctx);
				enterOuterAlt(_localctx, 52);
				{
				setState(840);
				match(SHOW);
				setState(841);
				match(SCHEMAS);
				setState(844);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(842);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(843);
					identifier();
					}
				}

				setState(852);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(846);
					match(LIKE);
					setState(847);
					((ShowSchemasContext)_localctx).pattern = string();
					setState(850);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(848);
						match(ESCAPE);
						setState(849);
						((ShowSchemasContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 53:
				_localctx = new ShowCatalogsContext(_localctx);
				enterOuterAlt(_localctx, 53);
				{
				setState(854);
				match(SHOW);
				setState(855);
				match(CATALOGS);
				setState(862);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(856);
					match(LIKE);
					setState(857);
					((ShowCatalogsContext)_localctx).pattern = string();
					setState(860);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(858);
						match(ESCAPE);
						setState(859);
						((ShowCatalogsContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 54:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 54);
				{
				setState(864);
				match(SHOW);
				setState(865);
				match(COLUMNS);
				setState(866);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(868);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -9203388181402353664L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967880946630052421L) != 0 || (((_la - 133)) & ~0x3f) == 0 && ((1L << (_la - 133)) & -778514395575617L) != 0 || (((_la - 198)) & ~0x3f) == 0 && ((1L << (_la - 198)) & 8592722952413183935L) != 0 || (((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2111062592315255L) != 0) {
					{
					setState(867);
					qualifiedName();
					}
				}

				setState(876);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(870);
					match(LIKE);
					setState(871);
					((ShowColumnsContext)_localctx).pattern = string();
					setState(874);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(872);
						match(ESCAPE);
						setState(873);
						((ShowColumnsContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 55:
				_localctx = new ShowStatsContext(_localctx);
				enterOuterAlt(_localctx, 55);
				{
				setState(878);
				match(SHOW);
				setState(879);
				match(STATS);
				setState(880);
				match(FOR);
				setState(881);
				qualifiedName();
				}
				break;
			case 56:
				_localctx = new ShowStatsForQueryContext(_localctx);
				enterOuterAlt(_localctx, 56);
				{
				setState(882);
				match(SHOW);
				setState(883);
				match(STATS);
				setState(884);
				match(FOR);
				setState(885);
				match(T__1);
				setState(886);
				query();
				setState(887);
				match(T__2);
				}
				break;
			case 57:
				_localctx = new ShowRolesContext(_localctx);
				enterOuterAlt(_localctx, 57);
				{
				setState(889);
				match(SHOW);
				setState(891);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CURRENT) {
					{
					setState(890);
					match(CURRENT);
					}
				}

				setState(893);
				match(ROLES);
				setState(896);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(894);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(895);
					identifier();
					}
				}

				}
				break;
			case 58:
				_localctx = new ShowRoleGrantsContext(_localctx);
				enterOuterAlt(_localctx, 58);
				{
				setState(898);
				match(SHOW);
				setState(899);
				match(ROLE);
				setState(900);
				match(GRANTS);
				setState(903);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(901);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(902);
					identifier();
					}
				}

				}
				break;
			case 59:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 59);
				{
				setState(905);
				match(DESCRIBE);
				setState(906);
				qualifiedName();
				}
				break;
			case 60:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 60);
				{
				setState(907);
				match(DESC);
				setState(908);
				qualifiedName();
				}
				break;
			case 61:
				_localctx = new ShowFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 61);
				{
				setState(909);
				match(SHOW);
				setState(910);
				match(FUNCTIONS);
				setState(917);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(911);
					match(LIKE);
					setState(912);
					((ShowFunctionsContext)_localctx).pattern = string();
					setState(915);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(913);
						match(ESCAPE);
						setState(914);
						((ShowFunctionsContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 62:
				_localctx = new ShowSessionContext(_localctx);
				enterOuterAlt(_localctx, 62);
				{
				setState(919);
				match(SHOW);
				setState(920);
				match(SESSION);
				setState(927);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(921);
					match(LIKE);
					setState(922);
					((ShowSessionContext)_localctx).pattern = string();
					setState(925);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(923);
						match(ESCAPE);
						setState(924);
						((ShowSessionContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 63:
				_localctx = new SetSessionContext(_localctx);
				enterOuterAlt(_localctx, 63);
				{
				setState(929);
				match(SET);
				setState(930);
				match(SESSION);
				setState(931);
				qualifiedName();
				setState(932);
				match(EQ);
				setState(933);
				expression();
				}
				break;
			case 64:
				_localctx = new ResetSessionContext(_localctx);
				enterOuterAlt(_localctx, 64);
				{
				setState(935);
				match(RESET);
				setState(936);
				match(SESSION);
				setState(937);
				qualifiedName();
				}
				break;
			case 65:
				_localctx = new StartTransactionContext(_localctx);
				enterOuterAlt(_localctx, 65);
				{
				setState(938);
				match(START);
				setState(939);
				match(TRANSACTION);
				setState(948);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ISOLATION || _la==READ) {
					{
					setState(940);
					transactionMode();
					setState(945);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(941);
						match(T__3);
						setState(942);
						transactionMode();
						}
						}
						setState(947);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 66:
				_localctx = new CommitContext(_localctx);
				enterOuterAlt(_localctx, 66);
				{
				setState(950);
				match(COMMIT);
				setState(952);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WORK) {
					{
					setState(951);
					match(WORK);
					}
				}

				}
				break;
			case 67:
				_localctx = new RollbackContext(_localctx);
				enterOuterAlt(_localctx, 67);
				{
				setState(954);
				match(ROLLBACK);
				setState(956);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WORK) {
					{
					setState(955);
					match(WORK);
					}
				}

				}
				break;
			case 68:
				_localctx = new PrepareContext(_localctx);
				enterOuterAlt(_localctx, 68);
				{
				setState(958);
				match(PREPARE);
				setState(959);
				identifier();
				setState(960);
				match(FROM);
				setState(961);
				statement();
				}
				break;
			case 69:
				_localctx = new DeallocateContext(_localctx);
				enterOuterAlt(_localctx, 69);
				{
				setState(963);
				match(DEALLOCATE);
				setState(964);
				match(PREPARE);
				setState(965);
				identifier();
				}
				break;
			case 70:
				_localctx = new ExecuteContext(_localctx);
				enterOuterAlt(_localctx, 70);
				{
				setState(966);
				match(EXECUTE);
				setState(967);
				identifier();
				setState(977);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(968);
					match(USING);
					setState(969);
					expression();
					setState(974);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(970);
						match(T__3);
						setState(971);
						expression();
						}
						}
						setState(976);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 71:
				_localctx = new DescribeInputContext(_localctx);
				enterOuterAlt(_localctx, 71);
				{
				setState(979);
				match(DESCRIBE);
				setState(980);
				match(INPUT);
				setState(981);
				identifier();
				}
				break;
			case 72:
				_localctx = new DescribeOutputContext(_localctx);
				enterOuterAlt(_localctx, 72);
				{
				setState(982);
				match(DESCRIBE);
				setState(983);
				match(OUTPUT);
				setState(984);
				identifier();
				}
				break;
			case 73:
				_localctx = new SetPathContext(_localctx);
				enterOuterAlt(_localctx, 73);
				{
				setState(985);
				match(SET);
				setState(986);
				match(PATH);
				setState(987);
				pathSpecification();
				}
				break;
			case 74:
				_localctx = new SetTimeZoneContext(_localctx);
				enterOuterAlt(_localctx, 74);
				{
				setState(988);
				match(SET);
				setState(989);
				match(TIME);
				setState(990);
				match(ZONE);
				setState(993);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,102,_ctx) ) {
				case 1:
					{
					setState(991);
					match(LOCAL);
					}
					break;
				case 2:
					{
					setState(992);
					expression();
					}
					break;
				}
				}
				break;
			case 75:
				_localctx = new UpdateContext(_localctx);
				enterOuterAlt(_localctx, 75);
				{
				setState(995);
				match(UPDATE);
				setState(996);
				qualifiedName();
				setState(997);
				match(SET);
				setState(998);
				updateAssignment();
				setState(1003);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(999);
					match(T__3);
					setState(1000);
					updateAssignment();
					}
					}
					setState(1005);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1008);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(1006);
					match(WHERE);
					setState(1007);
					((UpdateContext)_localctx).where = booleanExpression(0);
					}
				}

				}
				break;
			case 76:
				_localctx = new MergeContext(_localctx);
				enterOuterAlt(_localctx, 76);
				{
				setState(1010);
				match(MERGE);
				setState(1011);
				match(INTO);
				setState(1012);
				qualifiedName();
				setState(1017);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -9203388181133918208L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967880946630052421L) != 0 || (((_la - 133)) & ~0x3f) == 0 && ((1L << (_la - 133)) & -778514395575617L) != 0 || (((_la - 198)) & ~0x3f) == 0 && ((1L << (_la - 198)) & 8592722952413183935L) != 0 || (((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2111062592315255L) != 0) {
					{
					setState(1014);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1013);
						match(AS);
						}
					}

					setState(1016);
					identifier();
					}
				}

				setState(1019);
				match(USING);
				setState(1020);
				relation(0);
				setState(1021);
				match(ON);
				setState(1022);
				expression();
				setState(1024); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1023);
					mergeCase();
					}
					}
					setState(1026); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryContext extends ParserRuleContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public WithContext with() {
			return getRuleContext(WithContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1031);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1030);
				with();
				}
			}

			setState(1033);
			queryNoWith();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WithContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public TerminalNode RECURSIVE() { return getToken(SqlBaseParser.RECURSIVE, 0); }
		public WithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWith(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWith(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWith(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WithContext with() throws RecognitionException {
		WithContext _localctx = new WithContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_with);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1035);
			match(WITH);
			setState(1037);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RECURSIVE) {
				{
				setState(1036);
				match(RECURSIVE);
				}
			}

			setState(1039);
			namedQuery();
			setState(1044);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1040);
				match(T__3);
				setState(1041);
				namedQuery();
				}
				}
				setState(1046);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableElementContext extends ParserRuleContext {
		public ColumnDefinitionContext columnDefinition() {
			return getRuleContext(ColumnDefinitionContext.class,0);
		}
		public LikeClauseContext likeClause() {
			return getRuleContext(LikeClauseContext.class,0);
		}
		public TableElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableElement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableElement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableElementContext tableElement() throws RecognitionException {
		TableElementContext _localctx = new TableElementContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_tableElement);
		try {
			setState(1049);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DISTRIBUTED:
			case DOUBLE:
			case EMPTY:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERMUTE:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1047);
				columnDefinition();
				}
				break;
			case LIKE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1048);
				likeClause();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ColumnDefinitionContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public ColumnDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnDefinitionContext columnDefinition() throws RecognitionException {
		ColumnDefinitionContext _localctx = new ColumnDefinitionContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_columnDefinition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1051);
			identifier();
			setState(1052);
			type(0);
			setState(1055);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(1053);
				match(NOT);
				setState(1054);
				match(NULL);
				}
			}

			setState(1059);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1057);
				match(COMMENT);
				setState(1058);
				string();
				}
			}

			setState(1063);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1061);
				match(WITH);
				setState(1062);
				properties();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LikeClauseContext extends ParserRuleContext {
		public Token optionType;
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode INCLUDING() { return getToken(SqlBaseParser.INCLUDING, 0); }
		public TerminalNode EXCLUDING() { return getToken(SqlBaseParser.EXCLUDING, 0); }
		public LikeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_likeClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLikeClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLikeClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLikeClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LikeClauseContext likeClause() throws RecognitionException {
		LikeClauseContext _localctx = new LikeClauseContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_likeClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1065);
			match(LIKE);
			setState(1066);
			qualifiedName();
			setState(1069);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXCLUDING || _la==INCLUDING) {
				{
				setState(1067);
				((LikeClauseContext)_localctx).optionType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==EXCLUDING || _la==INCLUDING) ) {
					((LikeClauseContext)_localctx).optionType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1068);
				match(PROPERTIES);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertiesContext extends ParserRuleContext {
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public PropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_properties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertiesContext properties() throws RecognitionException {
		PropertiesContext _localctx = new PropertiesContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_properties);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1071);
			match(T__1);
			setState(1072);
			propertyAssignments();
			setState(1073);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyAssignmentsContext extends ParserRuleContext {
		public List<PropertyContext> property() {
			return getRuleContexts(PropertyContext.class);
		}
		public PropertyContext property(int i) {
			return getRuleContext(PropertyContext.class,i);
		}
		public PropertyAssignmentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyAssignments; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPropertyAssignments(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPropertyAssignments(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPropertyAssignments(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyAssignmentsContext propertyAssignments() throws RecognitionException {
		PropertyAssignmentsContext _localctx = new PropertyAssignmentsContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_propertyAssignments);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1075);
			property();
			setState(1080);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1076);
				match(T__3);
				setState(1077);
				property();
				}
				}
				setState(1082);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public PropertyValueContext propertyValue() {
			return getRuleContext(PropertyValueContext.class,0);
		}
		public PropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_property; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyContext property() throws RecognitionException {
		PropertyContext _localctx = new PropertyContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_property);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1083);
			identifier();
			setState(1084);
			match(EQ);
			setState(1085);
			propertyValue();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyValueContext extends ParserRuleContext {
		public PropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyValue; }
	 
		public PropertyValueContext() { }
		public void copyFrom(PropertyValueContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DefaultPropertyValueContext extends PropertyValueContext {
		public TerminalNode DEFAULT() { return getToken(SqlBaseParser.DEFAULT, 0); }
		public DefaultPropertyValueContext(PropertyValueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDefaultPropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDefaultPropertyValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDefaultPropertyValue(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NonDefaultPropertyValueContext extends PropertyValueContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public NonDefaultPropertyValueContext(PropertyValueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonDefaultPropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonDefaultPropertyValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonDefaultPropertyValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyValueContext propertyValue() throws RecognitionException {
		PropertyValueContext _localctx = new PropertyValueContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_propertyValue);
		try {
			setState(1089);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,118,_ctx) ) {
			case 1:
				_localctx = new DefaultPropertyValueContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1087);
				match(DEFAULT);
				}
				break;
			case 2:
				_localctx = new NonDefaultPropertyValueContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1088);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CatalogPropertiesContext extends ParserRuleContext {
		public CatalogPropertyAssignmentsContext catalogPropertyAssignments() {
			return getRuleContext(CatalogPropertyAssignmentsContext.class,0);
		}
		public CatalogPropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_catalogProperties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCatalogProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCatalogProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCatalogProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CatalogPropertiesContext catalogProperties() throws RecognitionException {
		CatalogPropertiesContext _localctx = new CatalogPropertiesContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_catalogProperties);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1091);
			match(T__1);
			setState(1092);
			catalogPropertyAssignments();
			setState(1093);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CatalogPropertyAssignmentsContext extends ParserRuleContext {
		public List<CatalogPropertyContext> catalogProperty() {
			return getRuleContexts(CatalogPropertyContext.class);
		}
		public CatalogPropertyContext catalogProperty(int i) {
			return getRuleContext(CatalogPropertyContext.class,i);
		}
		public CatalogPropertyAssignmentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_catalogPropertyAssignments; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCatalogPropertyAssignments(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCatalogPropertyAssignments(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCatalogPropertyAssignments(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CatalogPropertyAssignmentsContext catalogPropertyAssignments() throws RecognitionException {
		CatalogPropertyAssignmentsContext _localctx = new CatalogPropertyAssignmentsContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_catalogPropertyAssignments);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1095);
			catalogProperty();
			setState(1100);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1096);
				match(T__3);
				setState(1097);
				catalogProperty();
				}
				}
				setState(1102);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CatalogPropertyContext extends ParserRuleContext {
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public CatalogPropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_catalogProperty; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCatalogProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCatalogProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCatalogProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CatalogPropertyContext catalogProperty() throws RecognitionException {
		CatalogPropertyContext _localctx = new CatalogPropertyContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_catalogProperty);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1103);
			string();
			setState(1104);
			match(EQ);
			setState(1105);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryNoWithContext extends ParserRuleContext {
		public RowCountContext offset;
		public LimitRowCountContext limit;
		public RowCountContext fetchFirst;
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode OFFSET() { return getToken(SqlBaseParser.OFFSET, 0); }
		public List<RowCountContext> rowCount() {
			return getRuleContexts(RowCountContext.class);
		}
		public RowCountContext rowCount(int i) {
			return getRuleContext(RowCountContext.class,i);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode FETCH() { return getToken(SqlBaseParser.FETCH, 0); }
		public LimitRowCountContext limitRowCount() {
			return getRuleContext(LimitRowCountContext.class,0);
		}
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode NEXT() { return getToken(SqlBaseParser.NEXT, 0); }
		public List<TerminalNode> ROW() { return getTokens(SqlBaseParser.ROW); }
		public TerminalNode ROW(int i) {
			return getToken(SqlBaseParser.ROW, i);
		}
		public List<TerminalNode> ROWS() { return getTokens(SqlBaseParser.ROWS); }
		public TerminalNode ROWS(int i) {
			return getToken(SqlBaseParser.ROWS, i);
		}
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode TIES() { return getToken(SqlBaseParser.TIES, 0); }
		public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryNoWith; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryNoWith(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryNoWith(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryNoWith(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryNoWithContext queryNoWith() throws RecognitionException {
		QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_queryNoWith);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1107);
			queryTerm(0);
			setState(1118);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1108);
				match(ORDER);
				setState(1109);
				match(BY);
				setState(1110);
				sortItem();
				setState(1115);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1111);
					match(T__3);
					setState(1112);
					sortItem();
					}
					}
					setState(1117);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1125);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OFFSET) {
				{
				setState(1120);
				match(OFFSET);
				setState(1121);
				((QueryNoWithContext)_localctx).offset = rowCount();
				setState(1123);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW || _la==ROWS) {
					{
					setState(1122);
					_la = _input.LA(1);
					if ( !(_la==ROW || _la==ROWS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
			}

			setState(1140);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LIMIT:
				{
				{
				setState(1127);
				match(LIMIT);
				setState(1128);
				((QueryNoWithContext)_localctx).limit = limitRowCount();
				}
				}
				break;
			case FETCH:
				{
				{
				setState(1129);
				match(FETCH);
				setState(1130);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==NEXT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1132);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==QUESTION_MARK || _la==INTEGER_VALUE) {
					{
					setState(1131);
					((QueryNoWithContext)_localctx).fetchFirst = rowCount();
					}
				}

				setState(1134);
				_la = _input.LA(1);
				if ( !(_la==ROW || _la==ROWS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1138);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ONLY:
					{
					setState(1135);
					match(ONLY);
					}
					break;
				case WITH:
					{
					setState(1136);
					match(WITH);
					setState(1137);
					match(TIES);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				break;
			case EOF:
			case T__2:
			case WITH:
				break;
			default:
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LimitRowCountContext extends ParserRuleContext {
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public RowCountContext rowCount() {
			return getRuleContext(RowCountContext.class,0);
		}
		public LimitRowCountContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitRowCount; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLimitRowCount(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLimitRowCount(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLimitRowCount(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LimitRowCountContext limitRowCount() throws RecognitionException {
		LimitRowCountContext _localctx = new LimitRowCountContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_limitRowCount);
		try {
			setState(1144);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ALL:
				enterOuterAlt(_localctx, 1);
				{
				setState(1142);
				match(ALL);
				}
				break;
			case QUESTION_MARK:
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1143);
				rowCount();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RowCountContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode QUESTION_MARK() { return getToken(SqlBaseParser.QUESTION_MARK, 0); }
		public RowCountContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowCount; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowCount(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowCount(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowCount(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowCountContext rowCount() throws RecognitionException {
		RowCountContext _localctx = new RowCountContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_rowCount);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1146);
			_la = _input.LA(1);
			if ( !(_la==QUESTION_MARK || _la==INTEGER_VALUE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryTermDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryTermDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 42;
		enterRecursionRule(_localctx, 42, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1149);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(1165);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,131,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1163);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,130,_ctx) ) {
					case 1:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1151);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1152);
						((SetOperationContext)_localctx).operator = match(INTERSECT);
						setState(1154);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1153);
							setQuantifier();
							}
						}

						setState(1156);
						((SetOperationContext)_localctx).right = queryTerm(3);
						}
						break;
					case 2:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1157);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1158);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==EXCEPT || _la==UNION) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1160);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1159);
							setQuantifier();
							}
						}

						setState(1162);
						((SetOperationContext)_localctx).right = queryTerm(2);
						}
						break;
					}
					} 
				}
				setState(1167);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,131,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableContext extends QueryPrimaryContext {
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public InlineTableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_queryPrimary);
		try {
			int _alt;
			setState(1184);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1168);
				querySpecification();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1169);
				match(TABLE);
				setState(1170);
				qualifiedName();
				}
				break;
			case VALUES:
				_localctx = new InlineTableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1171);
				match(VALUES);
				setState(1172);
				expression();
				setState(1177);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,132,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1173);
						match(T__3);
						setState(1174);
						expression();
						}
						} 
					}
					setState(1179);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,132,_ctx);
				}
				}
				break;
			case T__1:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1180);
				match(T__1);
				setState(1181);
				queryNoWith();
				setState(1182);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrdering;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1186);
			expression();
			setState(1188);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1187);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1192);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(1190);
				match(NULLS);
				setState(1191);
				((SortItemContext)_localctx).nullOrdering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuerySpecificationContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public BooleanExpressionContext having;
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public List<SelectItemContext> selectItem() {
			return getRuleContexts(SelectItemContext.class);
		}
		public SelectItemContext selectItem(int i) {
			return getRuleContext(SelectItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public GroupByContext groupBy() {
			return getRuleContext(GroupByContext.class,0);
		}
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public List<WindowDefinitionContext> windowDefinition() {
			return getRuleContexts(WindowDefinitionContext.class);
		}
		public WindowDefinitionContext windowDefinition(int i) {
			return getRuleContext(WindowDefinitionContext.class,i);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_querySpecification);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1194);
			match(SELECT);
			setState(1196);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,136,_ctx) ) {
			case 1:
				{
				setState(1195);
				setQuantifier();
				}
				break;
			}
			setState(1198);
			selectItem();
			setState(1203);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,137,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1199);
					match(T__3);
					setState(1200);
					selectItem();
					}
					} 
				}
				setState(1205);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,137,_ctx);
			}
			setState(1215);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,139,_ctx) ) {
			case 1:
				{
				setState(1206);
				match(FROM);
				setState(1207);
				relation(0);
				setState(1212);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,138,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1208);
						match(T__3);
						setState(1209);
						relation(0);
						}
						} 
					}
					setState(1214);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,138,_ctx);
				}
				}
				break;
			}
			setState(1219);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,140,_ctx) ) {
			case 1:
				{
				setState(1217);
				match(WHERE);
				setState(1218);
				((QuerySpecificationContext)_localctx).where = booleanExpression(0);
				}
				break;
			}
			setState(1224);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,141,_ctx) ) {
			case 1:
				{
				setState(1221);
				match(GROUP);
				setState(1222);
				match(BY);
				setState(1223);
				groupBy();
				}
				break;
			}
			setState(1228);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,142,_ctx) ) {
			case 1:
				{
				setState(1226);
				match(HAVING);
				setState(1227);
				((QuerySpecificationContext)_localctx).having = booleanExpression(0);
				}
				break;
			}
			setState(1239);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,144,_ctx) ) {
			case 1:
				{
				setState(1230);
				match(WINDOW);
				setState(1231);
				windowDefinition();
				setState(1236);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,143,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1232);
						match(T__3);
						setState(1233);
						windowDefinition();
						}
						} 
					}
					setState(1238);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,143,_ctx);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupByContext extends ParserRuleContext {
		public List<GroupingElementContext> groupingElement() {
			return getRuleContexts(GroupingElementContext.class);
		}
		public GroupingElementContext groupingElement(int i) {
			return getRuleContext(GroupingElementContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public GroupByContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupBy; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupBy(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupBy(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupBy(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupByContext groupBy() throws RecognitionException {
		GroupByContext _localctx = new GroupByContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_groupBy);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1242);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,145,_ctx) ) {
			case 1:
				{
				setState(1241);
				setQuantifier();
				}
				break;
			}
			setState(1244);
			groupingElement();
			setState(1249);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,146,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1245);
					match(T__3);
					setState(1246);
					groupingElement();
					}
					} 
				}
				setState(1251);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,146,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupingElementContext extends ParserRuleContext {
		public GroupingElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingElement; }
	 
		public GroupingElementContext() { }
		public void copyFrom(GroupingElementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MultipleGroupingSetsContext extends GroupingElementContext {
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public MultipleGroupingSetsContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultipleGroupingSets(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultipleGroupingSets(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultipleGroupingSets(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SingleGroupingSetContext extends GroupingElementContext {
		public GroupingSetContext groupingSet() {
			return getRuleContext(GroupingSetContext.class,0);
		}
		public SingleGroupingSetContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CubeContext extends GroupingElementContext {
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public CubeContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCube(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCube(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCube(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RollupContext extends GroupingElementContext {
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public RollupContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRollup(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRollup(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRollup(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingElementContext groupingElement() throws RecognitionException {
		GroupingElementContext _localctx = new GroupingElementContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_groupingElement);
		int _la;
		try {
			setState(1292);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,152,_ctx) ) {
			case 1:
				_localctx = new SingleGroupingSetContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1252);
				groupingSet();
				}
				break;
			case 2:
				_localctx = new RollupContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1253);
				match(ROLLUP);
				setState(1254);
				match(T__1);
				setState(1263);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -304274493084532732L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967872150318926405L) != 0 || (((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -24910536512645121L) != 0 || (((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -9288743022235681L) != 0 || (((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 144048134953623005L) != 0) {
					{
					setState(1255);
					expression();
					setState(1260);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1256);
						match(T__3);
						setState(1257);
						expression();
						}
						}
						setState(1262);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1265);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new CubeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1266);
				match(CUBE);
				setState(1267);
				match(T__1);
				setState(1276);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -304274493084532732L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967872150318926405L) != 0 || (((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -24910536512645121L) != 0 || (((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -9288743022235681L) != 0 || (((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 144048134953623005L) != 0) {
					{
					setState(1268);
					expression();
					setState(1273);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1269);
						match(T__3);
						setState(1270);
						expression();
						}
						}
						setState(1275);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1278);
				match(T__2);
				}
				break;
			case 4:
				_localctx = new MultipleGroupingSetsContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1279);
				match(GROUPING);
				setState(1280);
				match(SETS);
				setState(1281);
				match(T__1);
				setState(1282);
				groupingSet();
				setState(1287);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1283);
					match(T__3);
					setState(1284);
					groupingSet();
					}
					}
					setState(1289);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1290);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_groupingSet);
		int _la;
		try {
			setState(1307);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,155,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1294);
				match(T__1);
				setState(1303);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -304274493084532732L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967872150318926405L) != 0 || (((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -24910536512645121L) != 0 || (((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -9288743022235681L) != 0 || (((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 144048134953623005L) != 0) {
					{
					setState(1295);
					expression();
					setState(1300);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1296);
						match(T__3);
						setState(1297);
						expression();
						}
						}
						setState(1302);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1305);
				match(T__2);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1306);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowDefinitionContext extends ParserRuleContext {
		public IdentifierContext name;
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public WindowSpecificationContext windowSpecification() {
			return getRuleContext(WindowSpecificationContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public WindowDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowDefinitionContext windowDefinition() throws RecognitionException {
		WindowDefinitionContext _localctx = new WindowDefinitionContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_windowDefinition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1309);
			((WindowDefinitionContext)_localctx).name = identifier();
			setState(1310);
			match(AS);
			setState(1311);
			match(T__1);
			setState(1312);
			windowSpecification();
			setState(1313);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowSpecificationContext extends ParserRuleContext {
		public IdentifierContext existingWindowName;
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WindowSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowSpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowSpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowSpecificationContext windowSpecification() throws RecognitionException {
		WindowSpecificationContext _localctx = new WindowSpecificationContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_windowSpecification);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1316);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,156,_ctx) ) {
			case 1:
				{
				setState(1315);
				((WindowSpecificationContext)_localctx).existingWindowName = identifier();
				}
				break;
			}
			setState(1328);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PARTITION) {
				{
				setState(1318);
				match(PARTITION);
				setState(1319);
				match(BY);
				setState(1320);
				((WindowSpecificationContext)_localctx).expression = expression();
				((WindowSpecificationContext)_localctx).partition.add(((WindowSpecificationContext)_localctx).expression);
				setState(1325);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1321);
					match(T__3);
					setState(1322);
					((WindowSpecificationContext)_localctx).expression = expression();
					((WindowSpecificationContext)_localctx).partition.add(((WindowSpecificationContext)_localctx).expression);
					}
					}
					setState(1327);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1340);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1330);
				match(ORDER);
				setState(1331);
				match(BY);
				setState(1332);
				sortItem();
				setState(1337);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1333);
					match(T__3);
					setState(1334);
					sortItem();
					}
					}
					setState(1339);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1343);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GROUPS || _la==MEASURES || _la==RANGE || _la==ROWS) {
				{
				setState(1342);
				windowFrame();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedQueryContext extends ParserRuleContext {
		public IdentifierContext name;
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1345);
			((NamedQueryContext)_localctx).name = identifier();
			setState(1347);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(1346);
				columnAliases();
				}
			}

			setState(1349);
			match(AS);
			setState(1350);
			match(T__1);
			setState(1351);
			query();
			setState(1352);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1354);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SelectItemContext extends ParserRuleContext {
		public SelectItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectItem; }
	 
		public SelectItemContext() { }
		public void copyFrom(SelectItemContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SelectAllContext extends SelectItemContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public SelectAllContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectAll(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectAll(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectAll(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SelectSingleContext extends SelectItemContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public SelectSingleContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectSingle(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectSingle(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectSingle(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectItemContext selectItem() throws RecognitionException {
		SelectItemContext _localctx = new SelectItemContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_selectItem);
		int _la;
		try {
			setState(1371);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,166,_ctx) ) {
			case 1:
				_localctx = new SelectSingleContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1356);
				expression();
				setState(1361);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,164,_ctx) ) {
				case 1:
					{
					setState(1358);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1357);
						match(AS);
						}
					}

					setState(1360);
					identifier();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1363);
				primaryExpression(0);
				setState(1364);
				match(T__0);
				setState(1365);
				match(ASTERISK);
				setState(1368);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,165,_ctx) ) {
				case 1:
					{
					setState(1366);
					match(AS);
					setState(1367);
					columnAliases();
					}
					break;
				}
				}
				break;
			case 3:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1370);
				match(ASTERISK);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationContext extends ParserRuleContext {
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
	 
		public RelationContext() { }
		public void copyFrom(RelationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RelationDefaultContext extends RelationContext {
		public SampledRelationContext sampledRelation() {
			return getRuleContext(SampledRelationContext.class,0);
		}
		public RelationDefaultContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRelationDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRelationDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRelationDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JoinRelationContext extends RelationContext {
		public RelationContext left;
		public SampledRelationContext right;
		public RelationContext rightRelation;
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public SampledRelationContext sampledRelation() {
			return getRuleContext(SampledRelationContext.class,0);
		}
		public JoinRelationContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		return relation(0);
	}

	private RelationContext relation(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		RelationContext _localctx = new RelationContext(_ctx, _parentState);
		RelationContext _prevctx = _localctx;
		int _startState = 66;
		enterRecursionRule(_localctx, 66, RULE_relation, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new RelationDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1374);
			sampledRelation();
			}
			_ctx.stop = _input.LT(-1);
			setState(1394);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,168,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new JoinRelationContext(new RelationContext(_parentctx, _parentState));
					((JoinRelationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_relation);
					setState(1376);
					if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
					setState(1390);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case CROSS:
						{
						setState(1377);
						match(CROSS);
						setState(1378);
						match(JOIN);
						setState(1379);
						((JoinRelationContext)_localctx).right = sampledRelation();
						}
						break;
					case FULL:
					case INNER:
					case JOIN:
					case LEFT:
					case RIGHT:
						{
						setState(1380);
						joinType();
						setState(1381);
						match(JOIN);
						setState(1382);
						((JoinRelationContext)_localctx).rightRelation = relation(0);
						setState(1383);
						joinCriteria();
						}
						break;
					case NATURAL:
						{
						setState(1385);
						match(NATURAL);
						setState(1386);
						joinType();
						setState(1387);
						match(JOIN);
						setState(1388);
						((JoinRelationContext)_localctx).right = sampledRelation();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					} 
				}
				setState(1396);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,168,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_joinType);
		int _la;
		try {
			setState(1412);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INNER:
			case JOIN:
				enterOuterAlt(_localctx, 1);
				{
				setState(1398);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(1397);
					match(INNER);
					}
				}

				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1400);
				match(LEFT);
				setState(1402);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1401);
					match(OUTER);
					}
				}

				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1404);
				match(RIGHT);
				setState(1406);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1405);
					match(OUTER);
					}
				}

				}
				break;
			case FULL:
				enterOuterAlt(_localctx, 4);
				{
				setState(1408);
				match(FULL);
				setState(1410);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1409);
					match(OUTER);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_joinCriteria);
		int _la;
		try {
			setState(1428);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1414);
				match(ON);
				setState(1415);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1416);
				match(USING);
				setState(1417);
				match(T__1);
				setState(1418);
				identifier();
				setState(1423);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1419);
					match(T__3);
					setState(1420);
					identifier();
					}
					}
					setState(1425);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1426);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SampledRelationContext extends ParserRuleContext {
		public ExpressionContext percentage;
		public PatternRecognitionContext patternRecognition() {
			return getRuleContext(PatternRecognitionContext.class,0);
		}
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public SampleTypeContext sampleType() {
			return getRuleContext(SampleTypeContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SampledRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampledRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampledRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampledRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampledRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampledRelationContext sampledRelation() throws RecognitionException {
		SampledRelationContext _localctx = new SampledRelationContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_sampledRelation);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1430);
			patternRecognition();
			setState(1437);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,176,_ctx) ) {
			case 1:
				{
				setState(1431);
				match(TABLESAMPLE);
				setState(1432);
				sampleType();
				setState(1433);
				match(T__1);
				setState(1434);
				((SampledRelationContext)_localctx).percentage = expression();
				setState(1435);
				match(T__2);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SampleTypeContext extends ParserRuleContext {
		public TerminalNode BERNOULLI() { return getToken(SqlBaseParser.BERNOULLI, 0); }
		public TerminalNode SYSTEM() { return getToken(SqlBaseParser.SYSTEM, 0); }
		public SampleTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampleType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampleType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleTypeContext sampleType() throws RecognitionException {
		SampleTypeContext _localctx = new SampleTypeContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_sampleType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1439);
			_la = _input.LA(1);
			if ( !(_la==BERNOULLI || _la==SYSTEM) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TrimsSpecificationContext extends ParserRuleContext {
		public TerminalNode LEADING() { return getToken(SqlBaseParser.LEADING, 0); }
		public TerminalNode TRAILING() { return getToken(SqlBaseParser.TRAILING, 0); }
		public TerminalNode BOTH() { return getToken(SqlBaseParser.BOTH, 0); }
		public TrimsSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_trimsSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTrimsSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTrimsSpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTrimsSpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TrimsSpecificationContext trimsSpecification() throws RecognitionException {
		TrimsSpecificationContext _localctx = new TrimsSpecificationContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_trimsSpecification);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1441);
			_la = _input.LA(1);
			if ( !(_la==BOTH || _la==LEADING || _la==TRAILING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ListAggOverflowBehaviorContext extends ParserRuleContext {
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public ListaggCountIndicationContext listaggCountIndication() {
			return getRuleContext(ListaggCountIndicationContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public ListAggOverflowBehaviorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listAggOverflowBehavior; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterListAggOverflowBehavior(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitListAggOverflowBehavior(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitListAggOverflowBehavior(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ListAggOverflowBehaviorContext listAggOverflowBehavior() throws RecognitionException {
		ListAggOverflowBehaviorContext _localctx = new ListAggOverflowBehaviorContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_listAggOverflowBehavior);
		int _la;
		try {
			setState(1449);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ERROR:
				enterOuterAlt(_localctx, 1);
				{
				setState(1443);
				match(ERROR);
				}
				break;
			case TRUNCATE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1444);
				match(TRUNCATE);
				setState(1446);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STRING || _la==UNICODE_STRING) {
					{
					setState(1445);
					string();
					}
				}

				setState(1448);
				listaggCountIndication();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ListaggCountIndicationContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode COUNT() { return getToken(SqlBaseParser.COUNT, 0); }
		public TerminalNode WITHOUT() { return getToken(SqlBaseParser.WITHOUT, 0); }
		public ListaggCountIndicationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listaggCountIndication; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterListaggCountIndication(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitListaggCountIndication(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitListaggCountIndication(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ListaggCountIndicationContext listaggCountIndication() throws RecognitionException {
		ListaggCountIndicationContext _localctx = new ListaggCountIndicationContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_listaggCountIndication);
		try {
			setState(1455);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case WITH:
				enterOuterAlt(_localctx, 1);
				{
				setState(1451);
				match(WITH);
				setState(1452);
				match(COUNT);
				}
				break;
			case WITHOUT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1453);
				match(WITHOUT);
				setState(1454);
				match(COUNT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PatternRecognitionContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public AliasedRelationContext aliasedRelation() {
			return getRuleContext(AliasedRelationContext.class,0);
		}
		public TerminalNode MATCH_RECOGNIZE() { return getToken(SqlBaseParser.MATCH_RECOGNIZE, 0); }
		public TerminalNode PATTERN() { return getToken(SqlBaseParser.PATTERN, 0); }
		public RowPatternContext rowPattern() {
			return getRuleContext(RowPatternContext.class,0);
		}
		public TerminalNode DEFINE() { return getToken(SqlBaseParser.DEFINE, 0); }
		public List<VariableDefinitionContext> variableDefinition() {
			return getRuleContexts(VariableDefinitionContext.class);
		}
		public VariableDefinitionContext variableDefinition(int i) {
			return getRuleContext(VariableDefinitionContext.class,i);
		}
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode MEASURES() { return getToken(SqlBaseParser.MEASURES, 0); }
		public List<MeasureDefinitionContext> measureDefinition() {
			return getRuleContexts(MeasureDefinitionContext.class);
		}
		public MeasureDefinitionContext measureDefinition(int i) {
			return getRuleContext(MeasureDefinitionContext.class,i);
		}
		public RowsPerMatchContext rowsPerMatch() {
			return getRuleContext(RowsPerMatchContext.class,0);
		}
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
		public SkipToContext skipTo() {
			return getRuleContext(SkipToContext.class,0);
		}
		public TerminalNode SUBSET() { return getToken(SqlBaseParser.SUBSET, 0); }
		public List<SubsetDefinitionContext> subsetDefinition() {
			return getRuleContexts(SubsetDefinitionContext.class);
		}
		public SubsetDefinitionContext subsetDefinition(int i) {
			return getRuleContext(SubsetDefinitionContext.class,i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode INITIAL() { return getToken(SqlBaseParser.INITIAL, 0); }
		public TerminalNode SEEK() { return getToken(SqlBaseParser.SEEK, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public PatternRecognitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_patternRecognition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternRecognition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternRecognition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternRecognition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PatternRecognitionContext patternRecognition() throws RecognitionException {
		PatternRecognitionContext _localctx = new PatternRecognitionContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_patternRecognition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1457);
			aliasedRelation();
			setState(1540);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,195,_ctx) ) {
			case 1:
				{
				setState(1458);
				match(MATCH_RECOGNIZE);
				setState(1459);
				match(T__1);
				setState(1470);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1460);
					match(PARTITION);
					setState(1461);
					match(BY);
					setState(1462);
					((PatternRecognitionContext)_localctx).expression = expression();
					((PatternRecognitionContext)_localctx).partition.add(((PatternRecognitionContext)_localctx).expression);
					setState(1467);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1463);
						match(T__3);
						setState(1464);
						((PatternRecognitionContext)_localctx).expression = expression();
						((PatternRecognitionContext)_localctx).partition.add(((PatternRecognitionContext)_localctx).expression);
						}
						}
						setState(1469);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1482);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(1472);
					match(ORDER);
					setState(1473);
					match(BY);
					setState(1474);
					sortItem();
					setState(1479);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1475);
						match(T__3);
						setState(1476);
						sortItem();
						}
						}
						setState(1481);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1493);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MEASURES) {
					{
					setState(1484);
					match(MEASURES);
					setState(1485);
					measureDefinition();
					setState(1490);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1486);
						match(T__3);
						setState(1487);
						measureDefinition();
						}
						}
						setState(1492);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1496);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ALL || _la==ONE) {
					{
					setState(1495);
					rowsPerMatch();
					}
				}

				setState(1501);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AFTER) {
					{
					setState(1498);
					match(AFTER);
					setState(1499);
					match(MATCH);
					setState(1500);
					skipTo();
					}
				}

				setState(1504);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INITIAL || _la==SEEK) {
					{
					setState(1503);
					_la = _input.LA(1);
					if ( !(_la==INITIAL || _la==SEEK) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(1506);
				match(PATTERN);
				setState(1507);
				match(T__1);
				setState(1508);
				rowPattern(0);
				setState(1509);
				match(T__2);
				setState(1519);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SUBSET) {
					{
					setState(1510);
					match(SUBSET);
					setState(1511);
					subsetDefinition();
					setState(1516);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1512);
						match(T__3);
						setState(1513);
						subsetDefinition();
						}
						}
						setState(1518);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1521);
				match(DEFINE);
				setState(1522);
				variableDefinition();
				setState(1527);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1523);
					match(T__3);
					setState(1524);
					variableDefinition();
					}
					}
					setState(1529);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1530);
				match(T__2);
				setState(1538);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,194,_ctx) ) {
				case 1:
					{
					setState(1532);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1531);
						match(AS);
						}
					}

					setState(1534);
					identifier();
					setState(1536);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,193,_ctx) ) {
					case 1:
						{
						setState(1535);
						columnAliases();
						}
						break;
					}
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MeasureDefinitionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public MeasureDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_measureDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMeasureDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMeasureDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMeasureDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MeasureDefinitionContext measureDefinition() throws RecognitionException {
		MeasureDefinitionContext _localctx = new MeasureDefinitionContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_measureDefinition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1542);
			expression();
			setState(1543);
			match(AS);
			setState(1544);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RowsPerMatchContext extends ParserRuleContext {
		public TerminalNode ONE() { return getToken(SqlBaseParser.ONE, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode PER() { return getToken(SqlBaseParser.PER, 0); }
		public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public EmptyMatchHandlingContext emptyMatchHandling() {
			return getRuleContext(EmptyMatchHandlingContext.class,0);
		}
		public RowsPerMatchContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowsPerMatch; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowsPerMatch(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowsPerMatch(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowsPerMatch(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowsPerMatchContext rowsPerMatch() throws RecognitionException {
		RowsPerMatchContext _localctx = new RowsPerMatchContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_rowsPerMatch);
		int _la;
		try {
			setState(1557);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1546);
				match(ONE);
				setState(1547);
				match(ROW);
				setState(1548);
				match(PER);
				setState(1549);
				match(MATCH);
				}
				break;
			case ALL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1550);
				match(ALL);
				setState(1551);
				match(ROWS);
				setState(1552);
				match(PER);
				setState(1553);
				match(MATCH);
				setState(1555);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OMIT || _la==SHOW || _la==WITH) {
					{
					setState(1554);
					emptyMatchHandling();
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class EmptyMatchHandlingContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode MATCHES() { return getToken(SqlBaseParser.MATCHES, 0); }
		public TerminalNode OMIT() { return getToken(SqlBaseParser.OMIT, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode UNMATCHED() { return getToken(SqlBaseParser.UNMATCHED, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public EmptyMatchHandlingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_emptyMatchHandling; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterEmptyMatchHandling(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitEmptyMatchHandling(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitEmptyMatchHandling(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EmptyMatchHandlingContext emptyMatchHandling() throws RecognitionException {
		EmptyMatchHandlingContext _localctx = new EmptyMatchHandlingContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_emptyMatchHandling);
		try {
			setState(1568);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SHOW:
				enterOuterAlt(_localctx, 1);
				{
				setState(1559);
				match(SHOW);
				setState(1560);
				match(EMPTY);
				setState(1561);
				match(MATCHES);
				}
				break;
			case OMIT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1562);
				match(OMIT);
				setState(1563);
				match(EMPTY);
				setState(1564);
				match(MATCHES);
				}
				break;
			case WITH:
				enterOuterAlt(_localctx, 3);
				{
				setState(1565);
				match(WITH);
				setState(1566);
				match(UNMATCHED);
				setState(1567);
				match(ROWS);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SkipToContext extends ParserRuleContext {
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode NEXT() { return getToken(SqlBaseParser.NEXT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode PAST() { return getToken(SqlBaseParser.PAST, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public SkipToContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_skipTo; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSkipTo(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSkipTo(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSkipTo(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SkipToContext skipTo() throws RecognitionException {
		SkipToContext _localctx = new SkipToContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_skipTo);
		try {
			setState(1589);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,199,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1570);
				match(T__4);
				setState(1571);
				match(TO);
				setState(1572);
				match(NEXT);
				setState(1573);
				match(ROW);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1574);
				match(T__4);
				setState(1575);
				match(PAST);
				setState(1576);
				match(LAST);
				setState(1577);
				match(ROW);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1578);
				match(T__4);
				setState(1579);
				match(TO);
				setState(1580);
				match(FIRST);
				setState(1581);
				identifier();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1582);
				match(T__4);
				setState(1583);
				match(TO);
				setState(1584);
				match(LAST);
				setState(1585);
				identifier();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1586);
				match(T__4);
				setState(1587);
				match(TO);
				setState(1588);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SubsetDefinitionContext extends ParserRuleContext {
		public IdentifierContext name;
		public IdentifierContext identifier;
		public List<IdentifierContext> union = new ArrayList<IdentifierContext>();
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public SubsetDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subsetDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubsetDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubsetDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubsetDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubsetDefinitionContext subsetDefinition() throws RecognitionException {
		SubsetDefinitionContext _localctx = new SubsetDefinitionContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_subsetDefinition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1591);
			((SubsetDefinitionContext)_localctx).name = identifier();
			setState(1592);
			match(EQ);
			setState(1593);
			match(T__1);
			setState(1594);
			((SubsetDefinitionContext)_localctx).identifier = identifier();
			((SubsetDefinitionContext)_localctx).union.add(((SubsetDefinitionContext)_localctx).identifier);
			setState(1599);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1595);
				match(T__3);
				setState(1596);
				((SubsetDefinitionContext)_localctx).identifier = identifier();
				((SubsetDefinitionContext)_localctx).union.add(((SubsetDefinitionContext)_localctx).identifier);
				}
				}
				setState(1601);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1602);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VariableDefinitionContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public VariableDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variableDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterVariableDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitVariableDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitVariableDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VariableDefinitionContext variableDefinition() throws RecognitionException {
		VariableDefinitionContext _localctx = new VariableDefinitionContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_variableDefinition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1604);
			identifier();
			setState(1605);
			match(AS);
			setState(1606);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AliasedRelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public AliasedRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliasedRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AliasedRelationContext aliasedRelation() throws RecognitionException {
		AliasedRelationContext _localctx = new AliasedRelationContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_aliasedRelation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1608);
			relationPrimary();
			setState(1616);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,203,_ctx) ) {
			case 1:
				{
				setState(1610);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(1609);
					match(AS);
					}
				}

				setState(1612);
				identifier();
				setState(1614);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,202,_ctx) ) {
				case 1:
					{
					setState(1613);
					columnAliases();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ColumnAliasesContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ColumnAliasesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnAliases; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnAliases(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnAliases(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnAliases(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnAliasesContext columnAliases() throws RecognitionException {
		ColumnAliasesContext _localctx = new ColumnAliasesContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_columnAliases);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1618);
			match(T__1);
			setState(1619);
			identifier();
			setState(1624);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1620);
				match(T__3);
				setState(1621);
				identifier();
				}
				}
				setState(1626);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1627);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryRelationContext extends RelationPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public ParenthesizedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnnestContext extends RelationPrimaryContext {
		public TerminalNode UNNEST() { return getToken(SqlBaseParser.UNNEST, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode ORDINALITY() { return getToken(SqlBaseParser.ORDINALITY, 0); }
		public UnnestContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnnest(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnnest(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnnest(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableFunctionInvocationContext extends RelationPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableFunctionCallContext tableFunctionCall() {
			return getRuleContext(TableFunctionCallContext.class,0);
		}
		public TableFunctionInvocationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableFunctionInvocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableFunctionInvocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableFunctionInvocation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LateralContext extends RelationPrimaryContext {
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public LateralContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLateral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLateral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLateral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableNameContext extends RelationPrimaryContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public QueryPeriodContext queryPeriod() {
			return getRuleContext(QueryPeriodContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_relationPrimary);
		int _la;
		try {
			setState(1666);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,208,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1629);
				qualifiedName();
				setState(1631);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,205,_ctx) ) {
				case 1:
					{
					setState(1630);
					queryPeriod();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new SubqueryRelationContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1633);
				match(T__1);
				setState(1634);
				query();
				setState(1635);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new UnnestContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1637);
				match(UNNEST);
				setState(1638);
				match(T__1);
				setState(1639);
				expression();
				setState(1644);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1640);
					match(T__3);
					setState(1641);
					expression();
					}
					}
					setState(1646);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1647);
				match(T__2);
				setState(1650);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,207,_ctx) ) {
				case 1:
					{
					setState(1648);
					match(WITH);
					setState(1649);
					match(ORDINALITY);
					}
					break;
				}
				}
				break;
			case 4:
				_localctx = new LateralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1652);
				match(LATERAL);
				setState(1653);
				match(T__1);
				setState(1654);
				query();
				setState(1655);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new TableFunctionInvocationContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1657);
				match(TABLE);
				setState(1658);
				match(T__1);
				setState(1659);
				tableFunctionCall();
				setState(1660);
				match(T__2);
				}
				break;
			case 6:
				_localctx = new ParenthesizedRelationContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1662);
				match(T__1);
				setState(1663);
				relation(0);
				setState(1664);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableFunctionCallContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<TableFunctionArgumentContext> tableFunctionArgument() {
			return getRuleContexts(TableFunctionArgumentContext.class);
		}
		public TableFunctionArgumentContext tableFunctionArgument(int i) {
			return getRuleContext(TableFunctionArgumentContext.class,i);
		}
		public TerminalNode COPARTITION() { return getToken(SqlBaseParser.COPARTITION, 0); }
		public List<CopartitionTablesContext> copartitionTables() {
			return getRuleContexts(CopartitionTablesContext.class);
		}
		public CopartitionTablesContext copartitionTables(int i) {
			return getRuleContext(CopartitionTablesContext.class,i);
		}
		public TableFunctionCallContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableFunctionCall; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableFunctionCallContext tableFunctionCall() throws RecognitionException {
		TableFunctionCallContext _localctx = new TableFunctionCallContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_tableFunctionCall);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1668);
			qualifiedName();
			setState(1669);
			match(T__1);
			setState(1678);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,210,_ctx) ) {
			case 1:
				{
				setState(1670);
				tableFunctionArgument();
				setState(1675);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1671);
					match(T__3);
					setState(1672);
					tableFunctionArgument();
					}
					}
					setState(1677);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			}
			setState(1689);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COPARTITION) {
				{
				setState(1680);
				match(COPARTITION);
				setState(1681);
				copartitionTables();
				setState(1686);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1682);
					match(T__3);
					setState(1683);
					copartitionTables();
					}
					}
					setState(1688);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1691);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableFunctionArgumentContext extends ParserRuleContext {
		public TableArgumentContext tableArgument() {
			return getRuleContext(TableArgumentContext.class,0);
		}
		public DescriptorArgumentContext descriptorArgument() {
			return getRuleContext(DescriptorArgumentContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TableFunctionArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableFunctionArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableFunctionArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableFunctionArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableFunctionArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableFunctionArgumentContext tableFunctionArgument() throws RecognitionException {
		TableFunctionArgumentContext _localctx = new TableFunctionArgumentContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_tableFunctionArgument);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1696);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,213,_ctx) ) {
			case 1:
				{
				setState(1693);
				identifier();
				setState(1694);
				match(T__5);
				}
				break;
			}
			setState(1701);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,214,_ctx) ) {
			case 1:
				{
				setState(1698);
				tableArgument();
				}
				break;
			case 2:
				{
				setState(1699);
				descriptorArgument();
				}
				break;
			case 3:
				{
				setState(1700);
				expression();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableArgumentContext extends ParserRuleContext {
		public TableArgumentRelationContext tableArgumentRelation() {
			return getRuleContext(TableArgumentRelationContext.class,0);
		}
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode PRUNE() { return getToken(SqlBaseParser.PRUNE, 0); }
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode KEEP() { return getToken(SqlBaseParser.KEEP, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TableArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableArgumentContext tableArgument() throws RecognitionException {
		TableArgumentContext _localctx = new TableArgumentContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_tableArgument);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1703);
			tableArgumentRelation();
			setState(1721);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PARTITION) {
				{
				setState(1704);
				match(PARTITION);
				setState(1705);
				match(BY);
				setState(1719);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,217,_ctx) ) {
				case 1:
					{
					setState(1706);
					match(T__1);
					setState(1715);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (((_la) & ~0x3f) == 0 && ((1L << _la) & -304274493084532732L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967872150318926405L) != 0 || (((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -24910536512645121L) != 0 || (((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -9288743022235681L) != 0 || (((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 144048134953623005L) != 0) {
						{
						setState(1707);
						expression();
						setState(1712);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__3) {
							{
							{
							setState(1708);
							match(T__3);
							setState(1709);
							expression();
							}
							}
							setState(1714);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(1717);
					match(T__2);
					}
					break;
				case 2:
					{
					setState(1718);
					expression();
					}
					break;
				}
				}
			}

			setState(1729);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PRUNE:
				{
				setState(1723);
				match(PRUNE);
				setState(1724);
				match(WHEN);
				setState(1725);
				match(EMPTY);
				}
				break;
			case KEEP:
				{
				setState(1726);
				match(KEEP);
				setState(1727);
				match(WHEN);
				setState(1728);
				match(EMPTY);
				}
				break;
			case T__2:
			case T__3:
			case COPARTITION:
			case ORDER:
				break;
			default:
				break;
			}
			setState(1747);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1731);
				match(ORDER);
				setState(1732);
				match(BY);
				setState(1745);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,221,_ctx) ) {
				case 1:
					{
					setState(1733);
					match(T__1);
					setState(1734);
					sortItem();
					setState(1739);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1735);
						match(T__3);
						setState(1736);
						sortItem();
						}
						}
						setState(1741);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1742);
					match(T__2);
					}
					break;
				case 2:
					{
					setState(1744);
					sortItem();
					}
					break;
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableArgumentRelationContext extends ParserRuleContext {
		public TableArgumentRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableArgumentRelation; }
	 
		public TableArgumentRelationContext() { }
		public void copyFrom(TableArgumentRelationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableArgumentQueryContext extends TableArgumentRelationContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public TableArgumentQueryContext(TableArgumentRelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableArgumentQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableArgumentQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableArgumentQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableArgumentTableContext extends TableArgumentRelationContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public TableArgumentTableContext(TableArgumentRelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableArgumentTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableArgumentTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableArgumentTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableArgumentRelationContext tableArgumentRelation() throws RecognitionException {
		TableArgumentRelationContext _localctx = new TableArgumentRelationContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_tableArgumentRelation);
		int _la;
		try {
			setState(1775);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,229,_ctx) ) {
			case 1:
				_localctx = new TableArgumentTableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1749);
				match(TABLE);
				setState(1750);
				match(T__1);
				setState(1751);
				qualifiedName();
				setState(1752);
				match(T__2);
				setState(1760);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,225,_ctx) ) {
				case 1:
					{
					setState(1754);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1753);
						match(AS);
						}
					}

					setState(1756);
					identifier();
					setState(1758);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__1) {
						{
						setState(1757);
						columnAliases();
						}
					}

					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new TableArgumentQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1762);
				match(TABLE);
				setState(1763);
				match(T__1);
				setState(1764);
				query();
				setState(1765);
				match(T__2);
				setState(1773);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,228,_ctx) ) {
				case 1:
					{
					setState(1767);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1766);
						match(AS);
						}
					}

					setState(1769);
					identifier();
					setState(1771);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__1) {
						{
						setState(1770);
						columnAliases();
						}
					}

					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DescriptorArgumentContext extends ParserRuleContext {
		public TerminalNode DESCRIPTOR() { return getToken(SqlBaseParser.DESCRIPTOR, 0); }
		public List<DescriptorFieldContext> descriptorField() {
			return getRuleContexts(DescriptorFieldContext.class);
		}
		public DescriptorFieldContext descriptorField(int i) {
			return getRuleContext(DescriptorFieldContext.class,i);
		}
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public DescriptorArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_descriptorArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescriptorArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescriptorArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescriptorArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescriptorArgumentContext descriptorArgument() throws RecognitionException {
		DescriptorArgumentContext _localctx = new DescriptorArgumentContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_descriptorArgument);
		int _la;
		try {
			setState(1795);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DESCRIPTOR:
				enterOuterAlt(_localctx, 1);
				{
				setState(1777);
				match(DESCRIPTOR);
				setState(1778);
				match(T__1);
				setState(1779);
				descriptorField();
				setState(1784);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1780);
					match(T__3);
					setState(1781);
					descriptorField();
					}
					}
					setState(1786);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1787);
				match(T__2);
				}
				break;
			case CAST:
				enterOuterAlt(_localctx, 2);
				{
				setState(1789);
				match(CAST);
				setState(1790);
				match(T__1);
				setState(1791);
				match(NULL);
				setState(1792);
				match(AS);
				setState(1793);
				match(DESCRIPTOR);
				setState(1794);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DescriptorFieldContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public DescriptorFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_descriptorField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescriptorField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescriptorField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescriptorField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescriptorFieldContext descriptorField() throws RecognitionException {
		DescriptorFieldContext _localctx = new DescriptorFieldContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_descriptorField);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1797);
			identifier();
			setState(1799);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((_la) & ~0x3f) == 0 && ((1L << _la) & -9203388181402353664L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967880946630052421L) != 0 || (((_la - 133)) & ~0x3f) == 0 && ((1L << (_la - 133)) & -778514395575617L) != 0 || (((_la - 198)) & ~0x3f) == 0 && ((1L << (_la - 198)) & 8592722952413183935L) != 0 || (((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2111062592315255L) != 0) {
				{
				setState(1798);
				type(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CopartitionTablesContext extends ParserRuleContext {
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public CopartitionTablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_copartitionTables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCopartitionTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCopartitionTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCopartitionTables(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CopartitionTablesContext copartitionTables() throws RecognitionException {
		CopartitionTablesContext _localctx = new CopartitionTablesContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_copartitionTables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1801);
			match(T__1);
			setState(1802);
			qualifiedName();
			setState(1803);
			match(T__3);
			setState(1804);
			qualifiedName();
			setState(1809);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1805);
				match(T__3);
				setState(1806);
				qualifiedName();
				}
				}
				setState(1811);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1812);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1814);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PredicatedContext extends BooleanExpressionContext {
		public ValueExpressionContext valueExpression;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class OrContext extends BooleanExpressionContext {
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public OrContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOr(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AndContext extends BooleanExpressionContext {
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public AndContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnd(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnd(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAnd(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 118;
		enterRecursionRule(_localctx, 118, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1823);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CASCADE:
			case CASE:
			case CAST:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case CURRENT_CATALOG:
			case CURRENT_DATE:
			case CURRENT_PATH:
			case CURRENT_SCHEMA:
			case CURRENT_TIME:
			case CURRENT_TIMESTAMP:
			case CURRENT_USER:
			case DATA:
			case DATE:
			case DAY:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DISTRIBUTED:
			case DOUBLE:
			case EMPTY:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXISTS:
			case EXPLAIN:
			case EXTRACT:
			case FALSE:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPING:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case JSON_ARRAY:
			case JSON_EXISTS:
			case JSON_OBJECT:
			case JSON_QUERY:
			case JSON_VALUE:
			case KEEP:
			case KEY:
			case KEYS:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEVEL:
			case LIMIT:
			case LISTAGG:
			case LOCAL:
			case LOCALTIME:
			case LOCALTIMESTAMP:
			case LOGICAL:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NORMALIZE:
			case NULL:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERMUTE:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRIM:
			case TRUE:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case PLUS:
			case MINUS:
			case QUESTION_MARK:
			case STRING:
			case UNICODE_STRING:
			case BINARY_LITERAL:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case DOUBLE_VALUE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				{
				_localctx = new PredicatedContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1817);
				((PredicatedContext)_localctx).valueExpression = valueExpression(0);
				setState(1819);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,234,_ctx) ) {
				case 1:
					{
					setState(1818);
					predicate(((PredicatedContext)_localctx).valueExpression);
					}
					break;
				}
				}
				break;
			case NOT:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1821);
				match(NOT);
				setState(1822);
				booleanExpression(3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(1833);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,237,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1831);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,236,_ctx) ) {
					case 1:
						{
						_localctx = new AndContext(new BooleanExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1825);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1826);
						match(AND);
						setState(1827);
						booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new OrContext(new BooleanExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1828);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1829);
						match(OR);
						setState(1830);
						booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(1835);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,237,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PredicateContext extends ParserRuleContext {
		public ParserRuleContext value;
		public PredicateContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
		public PredicateContext(ParserRuleContext parent, int invokingState, ParserRuleContext value) {
			super(parent, invokingState);
			this.value = value;
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
	 
		public PredicateContext() { }
		public void copyFrom(PredicateContext ctx) {
			super.copyFrom(ctx);
			this.value = ctx.value;
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonContext extends PredicateContext {
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LikeContext extends PredicateContext {
		public ValueExpressionContext pattern;
		public ValueExpressionContext escape;
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public LikeContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLike(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InSubqueryContext extends PredicateContext {
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public InSubqueryContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DistinctFromContext extends PredicateContext {
		public ValueExpressionContext right;
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public DistinctFromContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDistinctFrom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDistinctFrom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDistinctFrom(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InListContext extends PredicateContext {
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public InListContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInList(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullPredicateContext extends PredicateContext {
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public NullPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BetweenContext extends PredicateContext {
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BetweenContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBetween(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBetween(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBetween(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuantifiedComparisonContext extends PredicateContext {
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ComparisonQuantifierContext comparisonQuantifier() {
			return getRuleContext(ComparisonQuantifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public QuantifiedComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuantifiedComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuantifiedComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuantifiedComparison(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate(ParserRuleContext value) throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState(), value);
		enterRule(_localctx, 120, RULE_predicate);
		int _la;
		try {
			setState(1897);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,246,_ctx) ) {
			case 1:
				_localctx = new ComparisonContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1836);
				comparisonOperator();
				setState(1837);
				((ComparisonContext)_localctx).right = valueExpression(0);
				}
				break;
			case 2:
				_localctx = new QuantifiedComparisonContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1839);
				comparisonOperator();
				setState(1840);
				comparisonQuantifier();
				setState(1841);
				match(T__1);
				setState(1842);
				query();
				setState(1843);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new BetweenContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1846);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1845);
					match(NOT);
					}
				}

				setState(1848);
				match(BETWEEN);
				setState(1849);
				((BetweenContext)_localctx).lower = valueExpression(0);
				setState(1850);
				match(AND);
				setState(1851);
				((BetweenContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 4:
				_localctx = new InListContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1854);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1853);
					match(NOT);
					}
				}

				setState(1856);
				match(IN);
				setState(1857);
				match(T__1);
				setState(1858);
				expression();
				setState(1863);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1859);
					match(T__3);
					setState(1860);
					expression();
					}
					}
					setState(1865);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1866);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new InSubqueryContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1869);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1868);
					match(NOT);
					}
				}

				setState(1871);
				match(IN);
				setState(1872);
				match(T__1);
				setState(1873);
				query();
				setState(1874);
				match(T__2);
				}
				break;
			case 6:
				_localctx = new LikeContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1877);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1876);
					match(NOT);
					}
				}

				setState(1879);
				match(LIKE);
				setState(1880);
				((LikeContext)_localctx).pattern = valueExpression(0);
				setState(1883);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,243,_ctx) ) {
				case 1:
					{
					setState(1881);
					match(ESCAPE);
					setState(1882);
					((LikeContext)_localctx).escape = valueExpression(0);
					}
					break;
				}
				}
				break;
			case 7:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(1885);
				match(IS);
				setState(1887);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1886);
					match(NOT);
					}
				}

				setState(1889);
				match(NULL);
				}
				break;
			case 8:
				_localctx = new DistinctFromContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(1890);
				match(IS);
				setState(1892);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1891);
					match(NOT);
					}
				}

				setState(1894);
				match(DISTINCT);
				setState(1895);
				match(FROM);
				setState(1896);
				((DistinctFromContext)_localctx).right = valueExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ConcatenationContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public TerminalNode CONCAT() { return getToken(SqlBaseParser.CONCAT, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ConcatenationContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConcatenation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConcatenation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConcatenation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AtTimeZoneContext extends ValueExpressionContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TimeZoneSpecifierContext timeZoneSpecifier() {
			return getRuleContext(TimeZoneSpecifierContext.class,0);
		}
		public AtTimeZoneContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAtTimeZone(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAtTimeZone(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAtTimeZone(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 122;
		enterRecursionRule(_localctx, 122, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1903);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,247,_ctx) ) {
			case 1:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1900);
				primaryExpression(0);
				}
				break;
			case 2:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1901);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1902);
				valueExpression(4);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1919);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,249,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1917);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,248,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1905);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1906);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !((((_la - 298)) & ~0x3f) == 0 && ((1L << (_la - 298)) & 7L) != 0) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1907);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1908);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1909);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1910);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 3:
						{
						_localctx = new ConcatenationContext(new ValueExpressionContext(_parentctx, _parentState));
						((ConcatenationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1911);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1912);
						match(CONCAT);
						setState(1913);
						((ConcatenationContext)_localctx).right = valueExpression(2);
						}
						break;
					case 4:
						{
						_localctx = new AtTimeZoneContext(new ValueExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1914);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(1915);
						match(AT);
						setState(1916);
						timeZoneSpecifier();
						}
						break;
					}
					} 
				}
				setState(1921);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,249,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TypeConstructorContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode DOUBLE() { return getToken(SqlBaseParser.DOUBLE, 0); }
		public TerminalNode PRECISION() { return getToken(SqlBaseParser.PRECISION, 0); }
		public TypeConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonValueContext extends PrimaryExpressionContext {
		public JsonValueBehaviorContext emptyBehavior;
		public JsonValueBehaviorContext errorBehavior;
		public TerminalNode JSON_VALUE() { return getToken(SqlBaseParser.JSON_VALUE, 0); }
		public JsonPathInvocationContext jsonPathInvocation() {
			return getRuleContext(JsonPathInvocationContext.class,0);
		}
		public TerminalNode RETURNING() { return getToken(SqlBaseParser.RETURNING, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public List<TerminalNode> ON() { return getTokens(SqlBaseParser.ON); }
		public TerminalNode ON(int i) {
			return getToken(SqlBaseParser.ON, i);
		}
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public List<JsonValueBehaviorContext> jsonValueBehavior() {
			return getRuleContexts(JsonValueBehaviorContext.class);
		}
		public JsonValueBehaviorContext jsonValueBehavior(int i) {
			return getRuleContext(JsonValueBehaviorContext.class,i);
		}
		public JsonValueContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonValue(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SpecialDateTimeFunctionContext extends PrimaryExpressionContext {
		public Token name;
		public Token precision;
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIME() { return getToken(SqlBaseParser.CURRENT_TIME, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public TerminalNode LOCALTIME() { return getToken(SqlBaseParser.LOCALTIME, 0); }
		public TerminalNode LOCALTIMESTAMP() { return getToken(SqlBaseParser.LOCALTIMESTAMP, 0); }
		public SpecialDateTimeFunctionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSpecialDateTimeFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSpecialDateTimeFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSpecialDateTimeFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubstringContext extends PrimaryExpressionContext {
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public SubstringContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubstring(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubstring(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubstring(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCast(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LambdaContext extends PrimaryExpressionContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LambdaContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLambda(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLambda(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLambda(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TrimContext extends PrimaryExpressionContext {
		public ValueExpressionContext trimChar;
		public ValueExpressionContext trimSource;
		public TerminalNode TRIM() { return getToken(SqlBaseParser.TRIM, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TrimsSpecificationContext trimsSpecification() {
			return getRuleContext(TrimsSpecificationContext.class,0);
		}
		public TrimContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTrim(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTrim(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTrim(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParameterContext extends PrimaryExpressionContext {
		public TerminalNode QUESTION_MARK() { return getToken(SqlBaseParser.QUESTION_MARK, 0); }
		public ParameterContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParameter(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NormalizeContext extends PrimaryExpressionContext {
		public TerminalNode NORMALIZE() { return getToken(SqlBaseParser.NORMALIZE, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public NormalFormContext normalForm() {
			return getRuleContext(NormalFormContext.class,0);
		}
		public NormalizeContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNormalize(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNormalize(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNormalize(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonObjectContext extends PrimaryExpressionContext {
		public TerminalNode JSON_OBJECT() { return getToken(SqlBaseParser.JSON_OBJECT, 0); }
		public List<JsonObjectMemberContext> jsonObjectMember() {
			return getRuleContexts(JsonObjectMemberContext.class);
		}
		public JsonObjectMemberContext jsonObjectMember(int i) {
			return getRuleContext(JsonObjectMemberContext.class,i);
		}
		public TerminalNode RETURNING() { return getToken(SqlBaseParser.RETURNING, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public List<TerminalNode> NULL() { return getTokens(SqlBaseParser.NULL); }
		public TerminalNode NULL(int i) {
			return getToken(SqlBaseParser.NULL, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode ABSENT() { return getToken(SqlBaseParser.ABSENT, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode UNIQUE() { return getToken(SqlBaseParser.UNIQUE, 0); }
		public TerminalNode WITHOUT() { return getToken(SqlBaseParser.WITHOUT, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public JsonRepresentationContext jsonRepresentation() {
			return getRuleContext(JsonRepresentationContext.class,0);
		}
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public JsonObjectContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonObject(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonObject(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonObject(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntervalLiteralContext extends PrimaryExpressionContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralContext extends PrimaryExpressionContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BooleanLiteralContext extends PrimaryExpressionContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonArrayContext extends PrimaryExpressionContext {
		public TerminalNode JSON_ARRAY() { return getToken(SqlBaseParser.JSON_ARRAY, 0); }
		public List<JsonValueExpressionContext> jsonValueExpression() {
			return getRuleContexts(JsonValueExpressionContext.class);
		}
		public JsonValueExpressionContext jsonValueExpression(int i) {
			return getRuleContext(JsonValueExpressionContext.class,i);
		}
		public TerminalNode RETURNING() { return getToken(SqlBaseParser.RETURNING, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public List<TerminalNode> NULL() { return getTokens(SqlBaseParser.NULL); }
		public TerminalNode NULL(int i) {
			return getToken(SqlBaseParser.NULL, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode ABSENT() { return getToken(SqlBaseParser.ABSENT, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public JsonRepresentationContext jsonRepresentation() {
			return getRuleContext(JsonRepresentationContext.class,0);
		}
		public JsonArrayContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonArray(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonArray(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext operand;
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSimpleCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSimpleCase(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullLiteralContext extends PrimaryExpressionContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubscript(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonExistsContext extends PrimaryExpressionContext {
		public TerminalNode JSON_EXISTS() { return getToken(SqlBaseParser.JSON_EXISTS, 0); }
		public JsonPathInvocationContext jsonPathInvocation() {
			return getRuleContext(JsonPathInvocationContext.class,0);
		}
		public JsonExistsErrorBehaviorContext jsonExistsErrorBehavior() {
			return getRuleContext(JsonExistsErrorBehaviorContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public JsonExistsContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonExists(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentPathContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_PATH() { return getToken(SqlBaseParser.CURRENT_PATH, 0); }
		public CurrentPathContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentPath(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentPath(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentPath(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BinaryLiteralContext extends PrimaryExpressionContext {
		public TerminalNode BINARY_LITERAL() { return getToken(SqlBaseParser.BINARY_LITERAL, 0); }
		public BinaryLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBinaryLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBinaryLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBinaryLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentUserContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_USER() { return getToken(SqlBaseParser.CURRENT_USER, 0); }
		public CurrentUserContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentUser(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentUser(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentUser(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonQueryContext extends PrimaryExpressionContext {
		public JsonQueryBehaviorContext emptyBehavior;
		public JsonQueryBehaviorContext errorBehavior;
		public TerminalNode JSON_QUERY() { return getToken(SqlBaseParser.JSON_QUERY, 0); }
		public JsonPathInvocationContext jsonPathInvocation() {
			return getRuleContext(JsonPathInvocationContext.class,0);
		}
		public TerminalNode RETURNING() { return getToken(SqlBaseParser.RETURNING, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public JsonQueryWrapperBehaviorContext jsonQueryWrapperBehavior() {
			return getRuleContext(JsonQueryWrapperBehaviorContext.class,0);
		}
		public TerminalNode WRAPPER() { return getToken(SqlBaseParser.WRAPPER, 0); }
		public TerminalNode QUOTES() { return getToken(SqlBaseParser.QUOTES, 0); }
		public List<TerminalNode> ON() { return getTokens(SqlBaseParser.ON); }
		public TerminalNode ON(int i) {
			return getToken(SqlBaseParser.ON, i);
		}
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode KEEP() { return getToken(SqlBaseParser.KEEP, 0); }
		public TerminalNode OMIT() { return getToken(SqlBaseParser.OMIT, 0); }
		public List<JsonQueryBehaviorContext> jsonQueryBehavior() {
			return getRuleContexts(JsonQueryBehaviorContext.class);
		}
		public JsonQueryBehaviorContext jsonQueryBehavior(int i) {
			return getRuleContext(JsonQueryBehaviorContext.class,i);
		}
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public JsonRepresentationContext jsonRepresentation() {
			return getRuleContext(JsonRepresentationContext.class,0);
		}
		public TerminalNode SCALAR() { return getToken(SqlBaseParser.SCALAR, 0); }
		public TerminalNode TEXT_STRING() { return getToken(SqlBaseParser.TEXT_STRING, 0); }
		public JsonQueryContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MeasureContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public OverContext over() {
			return getRuleContext(OverContext.class,0);
		}
		public MeasureContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMeasure(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMeasure(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMeasure(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExtractContext extends PrimaryExpressionContext {
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ExtractContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExtract(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExtract(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExtract(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StringLiteralContext extends PrimaryExpressionContext {
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public StringLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArrayConstructorContext extends PrimaryExpressionContext {
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ArrayConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArrayConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArrayConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArrayConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public IdentifierContext label;
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public ProcessingModeContext processingMode() {
			return getRuleContext(ProcessingModeContext.class,0);
		}
		public FilterContext filter() {
			return getRuleContext(FilterContext.class,0);
		}
		public OverContext over() {
			return getRuleContext(OverContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public NullTreatmentContext nullTreatment() {
			return getRuleContext(NullTreatmentContext.class,0);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentSchemaContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_SCHEMA() { return getToken(SqlBaseParser.CURRENT_SCHEMA, 0); }
		public CurrentSchemaContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExistsContext extends PrimaryExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PositionContext extends PrimaryExpressionContext {
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public PositionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPosition(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ListaggContext extends PrimaryExpressionContext {
		public Token name;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode LISTAGG() { return getToken(SqlBaseParser.LISTAGG, 0); }
		public TerminalNode WITHIN() { return getToken(SqlBaseParser.WITHIN, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode OVERFLOW() { return getToken(SqlBaseParser.OVERFLOW, 0); }
		public ListAggOverflowBehaviorContext listAggOverflowBehavior() {
			return getRuleContext(ListAggOverflowBehaviorContext.class,0);
		}
		public ListaggContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterListagg(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitListagg(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitListagg(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSearchedCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSearchedCase(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentCatalogContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_CATALOG() { return getToken(SqlBaseParser.CURRENT_CATALOG, 0); }
		public CurrentCatalogContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentCatalog(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentCatalog(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentCatalog(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GroupingOperationContext extends PrimaryExpressionContext {
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public GroupingOperationContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 124;
		enterRecursionRule(_localctx, 124, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2372);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,312,_ctx) ) {
			case 1:
				{
				_localctx = new NullLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1923);
				match(NULL);
				}
				break;
			case 2:
				{
				_localctx = new IntervalLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1924);
				interval();
				}
				break;
			case 3:
				{
				_localctx = new TypeConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1925);
				identifier();
				setState(1926);
				string();
				}
				break;
			case 4:
				{
				_localctx = new TypeConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1928);
				match(DOUBLE);
				setState(1929);
				match(PRECISION);
				setState(1930);
				string();
				}
				break;
			case 5:
				{
				_localctx = new NumericLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1931);
				number();
				}
				break;
			case 6:
				{
				_localctx = new BooleanLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1932);
				booleanValue();
				}
				break;
			case 7:
				{
				_localctx = new StringLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1933);
				string();
				}
				break;
			case 8:
				{
				_localctx = new BinaryLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1934);
				match(BINARY_LITERAL);
				}
				break;
			case 9:
				{
				_localctx = new ParameterContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1935);
				match(QUESTION_MARK);
				}
				break;
			case 10:
				{
				_localctx = new PositionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1936);
				match(POSITION);
				setState(1937);
				match(T__1);
				setState(1938);
				valueExpression(0);
				setState(1939);
				match(IN);
				setState(1940);
				valueExpression(0);
				setState(1941);
				match(T__2);
				}
				break;
			case 11:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1943);
				match(T__1);
				setState(1944);
				expression();
				setState(1947); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1945);
					match(T__3);
					setState(1946);
					expression();
					}
					}
					setState(1949); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__3 );
				setState(1951);
				match(T__2);
				}
				break;
			case 12:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1953);
				match(ROW);
				setState(1954);
				match(T__1);
				setState(1955);
				expression();
				setState(1960);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1956);
					match(T__3);
					setState(1957);
					expression();
					}
					}
					setState(1962);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1963);
				match(T__2);
				}
				break;
			case 13:
				{
				_localctx = new ListaggContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1965);
				((ListaggContext)_localctx).name = match(LISTAGG);
				setState(1966);
				match(T__1);
				setState(1968);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,252,_ctx) ) {
				case 1:
					{
					setState(1967);
					setQuantifier();
					}
					break;
				}
				setState(1970);
				expression();
				setState(1973);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(1971);
					match(T__3);
					setState(1972);
					string();
					}
				}

				setState(1978);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(1975);
					match(ON);
					setState(1976);
					match(OVERFLOW);
					setState(1977);
					listAggOverflowBehavior();
					}
				}

				setState(1980);
				match(T__2);
				{
				setState(1981);
				match(WITHIN);
				setState(1982);
				match(GROUP);
				setState(1983);
				match(T__1);
				setState(1984);
				match(ORDER);
				setState(1985);
				match(BY);
				setState(1986);
				sortItem();
				setState(1991);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1987);
					match(T__3);
					setState(1988);
					sortItem();
					}
					}
					setState(1993);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1994);
				match(T__2);
				}
				}
				break;
			case 14:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1997);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,256,_ctx) ) {
				case 1:
					{
					setState(1996);
					processingMode();
					}
					break;
				}
				setState(1999);
				qualifiedName();
				setState(2000);
				match(T__1);
				setState(2004);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -9203388181402353664L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967880946630052421L) != 0 || (((_la - 133)) & ~0x3f) == 0 && ((1L << (_la - 133)) & -778514395575617L) != 0 || (((_la - 198)) & ~0x3f) == 0 && ((1L << (_la - 198)) & 8592722952413183935L) != 0 || (((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2111062592315255L) != 0) {
					{
					setState(2001);
					((FunctionCallContext)_localctx).label = identifier();
					setState(2002);
					match(T__0);
					}
				}

				setState(2006);
				match(ASTERISK);
				setState(2007);
				match(T__2);
				setState(2009);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,258,_ctx) ) {
				case 1:
					{
					setState(2008);
					filter();
					}
					break;
				}
				setState(2012);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,259,_ctx) ) {
				case 1:
					{
					setState(2011);
					over();
					}
					break;
				}
				}
				break;
			case 15:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2015);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,260,_ctx) ) {
				case 1:
					{
					setState(2014);
					processingMode();
					}
					break;
				}
				setState(2017);
				qualifiedName();
				setState(2018);
				match(T__1);
				setState(2030);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -304274493084532732L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967872150318924357L) != 0 || (((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -24910536512645121L) != 0 || (((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -9288743022235681L) != 0 || (((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 144048134953623005L) != 0) {
					{
					setState(2020);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,261,_ctx) ) {
					case 1:
						{
						setState(2019);
						setQuantifier();
						}
						break;
					}
					setState(2022);
					expression();
					setState(2027);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2023);
						match(T__3);
						setState(2024);
						expression();
						}
						}
						setState(2029);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2042);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(2032);
					match(ORDER);
					setState(2033);
					match(BY);
					setState(2034);
					sortItem();
					setState(2039);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2035);
						match(T__3);
						setState(2036);
						sortItem();
						}
						}
						setState(2041);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2044);
				match(T__2);
				setState(2046);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,266,_ctx) ) {
				case 1:
					{
					setState(2045);
					filter();
					}
					break;
				}
				setState(2052);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,268,_ctx) ) {
				case 1:
					{
					setState(2049);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IGNORE || _la==RESPECT) {
						{
						setState(2048);
						nullTreatment();
						}
					}

					setState(2051);
					over();
					}
					break;
				}
				}
				break;
			case 16:
				{
				_localctx = new MeasureContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2054);
				identifier();
				setState(2055);
				over();
				}
				break;
			case 17:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2057);
				identifier();
				setState(2058);
				match(T__6);
				setState(2059);
				expression();
				}
				break;
			case 18:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2061);
				match(T__1);
				setState(2070);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -9203388181402353664L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967880946630052421L) != 0 || (((_la - 133)) & ~0x3f) == 0 && ((1L << (_la - 133)) & -778514395575617L) != 0 || (((_la - 198)) & ~0x3f) == 0 && ((1L << (_la - 198)) & 8592722952413183935L) != 0 || (((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2111062592315255L) != 0) {
					{
					setState(2062);
					identifier();
					setState(2067);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2063);
						match(T__3);
						setState(2064);
						identifier();
						}
						}
						setState(2069);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2072);
				match(T__2);
				setState(2073);
				match(T__6);
				setState(2074);
				expression();
				}
				break;
			case 19:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2075);
				match(T__1);
				setState(2076);
				query();
				setState(2077);
				match(T__2);
				}
				break;
			case 20:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2079);
				match(EXISTS);
				setState(2080);
				match(T__1);
				setState(2081);
				query();
				setState(2082);
				match(T__2);
				}
				break;
			case 21:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2084);
				match(CASE);
				setState(2085);
				((SimpleCaseContext)_localctx).operand = expression();
				setState(2087); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2086);
					whenClause();
					}
					}
					setState(2089); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2093);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2091);
					match(ELSE);
					setState(2092);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2095);
				match(END);
				}
				break;
			case 22:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2097);
				match(CASE);
				setState(2099); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2098);
					whenClause();
					}
					}
					setState(2101); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2105);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2103);
					match(ELSE);
					setState(2104);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2107);
				match(END);
				}
				break;
			case 23:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2109);
				match(CAST);
				setState(2110);
				match(T__1);
				setState(2111);
				expression();
				setState(2112);
				match(AS);
				setState(2113);
				type(0);
				setState(2114);
				match(T__2);
				}
				break;
			case 24:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2116);
				match(TRY_CAST);
				setState(2117);
				match(T__1);
				setState(2118);
				expression();
				setState(2119);
				match(AS);
				setState(2120);
				type(0);
				setState(2121);
				match(T__2);
				}
				break;
			case 25:
				{
				_localctx = new ArrayConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2123);
				match(ARRAY);
				setState(2124);
				match(T__7);
				setState(2133);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -304274493084532732L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967872150318926405L) != 0 || (((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -24910536512645121L) != 0 || (((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -9288743022235681L) != 0 || (((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 144048134953623005L) != 0) {
					{
					setState(2125);
					expression();
					setState(2130);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2126);
						match(T__3);
						setState(2127);
						expression();
						}
						}
						setState(2132);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2135);
				match(T__8);
				}
				break;
			case 26:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2136);
				identifier();
				}
				break;
			case 27:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2137);
				((SpecialDateTimeFunctionContext)_localctx).name = match(CURRENT_DATE);
				}
				break;
			case 28:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2138);
				((SpecialDateTimeFunctionContext)_localctx).name = match(CURRENT_TIME);
				setState(2142);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,277,_ctx) ) {
				case 1:
					{
					setState(2139);
					match(T__1);
					setState(2140);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(2141);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 29:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2144);
				((SpecialDateTimeFunctionContext)_localctx).name = match(CURRENT_TIMESTAMP);
				setState(2148);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,278,_ctx) ) {
				case 1:
					{
					setState(2145);
					match(T__1);
					setState(2146);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(2147);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 30:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2150);
				((SpecialDateTimeFunctionContext)_localctx).name = match(LOCALTIME);
				setState(2154);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,279,_ctx) ) {
				case 1:
					{
					setState(2151);
					match(T__1);
					setState(2152);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(2153);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 31:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2156);
				((SpecialDateTimeFunctionContext)_localctx).name = match(LOCALTIMESTAMP);
				setState(2160);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,280,_ctx) ) {
				case 1:
					{
					setState(2157);
					match(T__1);
					setState(2158);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(2159);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 32:
				{
				_localctx = new CurrentUserContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2162);
				((CurrentUserContext)_localctx).name = match(CURRENT_USER);
				}
				break;
			case 33:
				{
				_localctx = new CurrentCatalogContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2163);
				((CurrentCatalogContext)_localctx).name = match(CURRENT_CATALOG);
				}
				break;
			case 34:
				{
				_localctx = new CurrentSchemaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2164);
				((CurrentSchemaContext)_localctx).name = match(CURRENT_SCHEMA);
				}
				break;
			case 35:
				{
				_localctx = new CurrentPathContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2165);
				((CurrentPathContext)_localctx).name = match(CURRENT_PATH);
				}
				break;
			case 36:
				{
				_localctx = new TrimContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2166);
				match(TRIM);
				setState(2167);
				match(T__1);
				setState(2175);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,283,_ctx) ) {
				case 1:
					{
					setState(2169);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,281,_ctx) ) {
					case 1:
						{
						setState(2168);
						trimsSpecification();
						}
						break;
					}
					setState(2172);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (((_la) & ~0x3f) == 0 && ((1L << _la) & -304274493084532732L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967872150318926405L) != 0 || (((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -24911086268459009L) != 0 || (((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -9288743022235681L) != 0 || (((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 144048134953623005L) != 0) {
						{
						setState(2171);
						((TrimContext)_localctx).trimChar = valueExpression(0);
						}
					}

					setState(2174);
					match(FROM);
					}
					break;
				}
				setState(2177);
				((TrimContext)_localctx).trimSource = valueExpression(0);
				setState(2178);
				match(T__2);
				}
				break;
			case 37:
				{
				_localctx = new TrimContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2180);
				match(TRIM);
				setState(2181);
				match(T__1);
				setState(2182);
				((TrimContext)_localctx).trimSource = valueExpression(0);
				setState(2183);
				match(T__3);
				setState(2184);
				((TrimContext)_localctx).trimChar = valueExpression(0);
				setState(2185);
				match(T__2);
				}
				break;
			case 38:
				{
				_localctx = new SubstringContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2187);
				match(SUBSTRING);
				setState(2188);
				match(T__1);
				setState(2189);
				valueExpression(0);
				setState(2190);
				match(FROM);
				setState(2191);
				valueExpression(0);
				setState(2194);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FOR) {
					{
					setState(2192);
					match(FOR);
					setState(2193);
					valueExpression(0);
					}
				}

				setState(2196);
				match(T__2);
				}
				break;
			case 39:
				{
				_localctx = new NormalizeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2198);
				match(NORMALIZE);
				setState(2199);
				match(T__1);
				setState(2200);
				valueExpression(0);
				setState(2203);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(2201);
					match(T__3);
					setState(2202);
					normalForm();
					}
				}

				setState(2205);
				match(T__2);
				}
				break;
			case 40:
				{
				_localctx = new ExtractContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2207);
				match(EXTRACT);
				setState(2208);
				match(T__1);
				setState(2209);
				identifier();
				setState(2210);
				match(FROM);
				setState(2211);
				valueExpression(0);
				setState(2212);
				match(T__2);
				}
				break;
			case 41:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2214);
				match(T__1);
				setState(2215);
				expression();
				setState(2216);
				match(T__2);
				}
				break;
			case 42:
				{
				_localctx = new GroupingOperationContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2218);
				match(GROUPING);
				setState(2219);
				match(T__1);
				setState(2228);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((_la) & ~0x3f) == 0 && ((1L << _la) & -9203388181402353664L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967880946630052421L) != 0 || (((_la - 133)) & ~0x3f) == 0 && ((1L << (_la - 133)) & -778514395575617L) != 0 || (((_la - 198)) & ~0x3f) == 0 && ((1L << (_la - 198)) & 8592722952413183935L) != 0 || (((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2111062592315255L) != 0) {
					{
					setState(2220);
					qualifiedName();
					setState(2225);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2221);
						match(T__3);
						setState(2222);
						qualifiedName();
						}
						}
						setState(2227);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2230);
				match(T__2);
				}
				break;
			case 43:
				{
				_localctx = new JsonExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2231);
				match(JSON_EXISTS);
				setState(2232);
				match(T__1);
				setState(2233);
				jsonPathInvocation();
				setState(2238);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ERROR || _la==FALSE || _la==TRUE || _la==UNKNOWN) {
					{
					setState(2234);
					jsonExistsErrorBehavior();
					setState(2235);
					match(ON);
					setState(2236);
					match(ERROR);
					}
				}

				setState(2240);
				match(T__2);
				}
				break;
			case 44:
				{
				_localctx = new JsonValueContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2242);
				match(JSON_VALUE);
				setState(2243);
				match(T__1);
				setState(2244);
				jsonPathInvocation();
				setState(2247);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RETURNING) {
					{
					setState(2245);
					match(RETURNING);
					setState(2246);
					type(0);
					}
				}

				setState(2253);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,290,_ctx) ) {
				case 1:
					{
					setState(2249);
					((JsonValueContext)_localctx).emptyBehavior = jsonValueBehavior();
					setState(2250);
					match(ON);
					setState(2251);
					match(EMPTY);
					}
					break;
				}
				setState(2259);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DEFAULT || _la==ERROR || _la==NULL) {
					{
					setState(2255);
					((JsonValueContext)_localctx).errorBehavior = jsonValueBehavior();
					setState(2256);
					match(ON);
					setState(2257);
					match(ERROR);
					}
				}

				setState(2261);
				match(T__2);
				}
				break;
			case 45:
				{
				_localctx = new JsonQueryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2263);
				match(JSON_QUERY);
				setState(2264);
				match(T__1);
				setState(2265);
				jsonPathInvocation();
				setState(2272);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RETURNING) {
					{
					setState(2266);
					match(RETURNING);
					setState(2267);
					type(0);
					setState(2270);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==FORMAT) {
						{
						setState(2268);
						match(FORMAT);
						setState(2269);
						jsonRepresentation();
						}
					}

					}
				}

				setState(2277);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH || _la==WITHOUT) {
					{
					setState(2274);
					jsonQueryWrapperBehavior();
					setState(2275);
					match(WRAPPER);
					}
				}

				setState(2286);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==KEEP || _la==OMIT) {
					{
					setState(2279);
					_la = _input.LA(1);
					if ( !(_la==KEEP || _la==OMIT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(2280);
					match(QUOTES);
					setState(2284);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ON) {
						{
						setState(2281);
						match(ON);
						setState(2282);
						match(SCALAR);
						setState(2283);
						match(TEXT_STRING);
						}
					}

					}
				}

				setState(2292);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,297,_ctx) ) {
				case 1:
					{
					setState(2288);
					((JsonQueryContext)_localctx).emptyBehavior = jsonQueryBehavior();
					setState(2289);
					match(ON);
					setState(2290);
					match(EMPTY);
					}
					break;
				}
				setState(2298);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EMPTY || _la==ERROR || _la==NULL) {
					{
					setState(2294);
					((JsonQueryContext)_localctx).errorBehavior = jsonQueryBehavior();
					setState(2295);
					match(ON);
					setState(2296);
					match(ERROR);
					}
				}

				setState(2300);
				match(T__2);
				}
				break;
			case 46:
				{
				_localctx = new JsonObjectContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2302);
				match(JSON_OBJECT);
				setState(2303);
				match(T__1);
				setState(2332);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,304,_ctx) ) {
				case 1:
					{
					setState(2304);
					jsonObjectMember();
					setState(2309);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2305);
						match(T__3);
						setState(2306);
						jsonObjectMember();
						}
						}
						setState(2311);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2318);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case NULL:
						{
						setState(2312);
						match(NULL);
						setState(2313);
						match(ON);
						setState(2314);
						match(NULL);
						}
						break;
					case ABSENT:
						{
						setState(2315);
						match(ABSENT);
						setState(2316);
						match(ON);
						setState(2317);
						match(NULL);
						}
						break;
					case T__2:
					case RETURNING:
					case WITH:
					case WITHOUT:
						break;
					default:
						break;
					}
					setState(2330);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case WITH:
						{
						setState(2320);
						match(WITH);
						setState(2321);
						match(UNIQUE);
						setState(2323);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==KEYS) {
							{
							setState(2322);
							match(KEYS);
							}
						}

						}
						break;
					case WITHOUT:
						{
						setState(2325);
						match(WITHOUT);
						setState(2326);
						match(UNIQUE);
						setState(2328);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==KEYS) {
							{
							setState(2327);
							match(KEYS);
							}
						}

						}
						break;
					case T__2:
					case RETURNING:
						break;
					default:
						break;
					}
					}
					break;
				}
				setState(2340);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RETURNING) {
					{
					setState(2334);
					match(RETURNING);
					setState(2335);
					type(0);
					setState(2338);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==FORMAT) {
						{
						setState(2336);
						match(FORMAT);
						setState(2337);
						jsonRepresentation();
						}
					}

					}
				}

				setState(2342);
				match(T__2);
				}
				break;
			case 47:
				{
				_localctx = new JsonArrayContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2343);
				match(JSON_ARRAY);
				setState(2344);
				match(T__1);
				setState(2361);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,309,_ctx) ) {
				case 1:
					{
					setState(2345);
					jsonValueExpression();
					setState(2350);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2346);
						match(T__3);
						setState(2347);
						jsonValueExpression();
						}
						}
						setState(2352);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2359);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case NULL:
						{
						setState(2353);
						match(NULL);
						setState(2354);
						match(ON);
						setState(2355);
						match(NULL);
						}
						break;
					case ABSENT:
						{
						setState(2356);
						match(ABSENT);
						setState(2357);
						match(ON);
						setState(2358);
						match(NULL);
						}
						break;
					case T__2:
					case RETURNING:
						break;
					default:
						break;
					}
					}
					break;
				}
				setState(2369);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RETURNING) {
					{
					setState(2363);
					match(RETURNING);
					setState(2364);
					type(0);
					setState(2367);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==FORMAT) {
						{
						setState(2365);
						match(FORMAT);
						setState(2366);
						jsonRepresentation();
						}
					}

					}
				}

				setState(2371);
				match(T__2);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2384);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,314,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2382);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,313,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2374);
						if (!(precpred(_ctx, 24))) throw new FailedPredicateException(this, "precpred(_ctx, 24)");
						setState(2375);
						match(T__7);
						setState(2376);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(2377);
						match(T__8);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2379);
						if (!(precpred(_ctx, 22))) throw new FailedPredicateException(this, "precpred(_ctx, 22)");
						setState(2380);
						match(T__0);
						setState(2381);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(2386);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,314,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonPathInvocationContext extends ParserRuleContext {
		public StringContext path;
		public JsonValueExpressionContext jsonValueExpression() {
			return getRuleContext(JsonValueExpressionContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode PASSING() { return getToken(SqlBaseParser.PASSING, 0); }
		public List<JsonArgumentContext> jsonArgument() {
			return getRuleContexts(JsonArgumentContext.class);
		}
		public JsonArgumentContext jsonArgument(int i) {
			return getRuleContext(JsonArgumentContext.class,i);
		}
		public JsonPathInvocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonPathInvocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonPathInvocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonPathInvocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonPathInvocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonPathInvocationContext jsonPathInvocation() throws RecognitionException {
		JsonPathInvocationContext _localctx = new JsonPathInvocationContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_jsonPathInvocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2387);
			jsonValueExpression();
			setState(2388);
			match(T__3);
			setState(2389);
			((JsonPathInvocationContext)_localctx).path = string();
			setState(2399);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PASSING) {
				{
				setState(2390);
				match(PASSING);
				setState(2391);
				jsonArgument();
				setState(2396);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2392);
					match(T__3);
					setState(2393);
					jsonArgument();
					}
					}
					setState(2398);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonValueExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public JsonRepresentationContext jsonRepresentation() {
			return getRuleContext(JsonRepresentationContext.class,0);
		}
		public JsonValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonValueExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonValueExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonValueExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonValueExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonValueExpressionContext jsonValueExpression() throws RecognitionException {
		JsonValueExpressionContext _localctx = new JsonValueExpressionContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_jsonValueExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2401);
			expression();
			setState(2404);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FORMAT) {
				{
				setState(2402);
				match(FORMAT);
				setState(2403);
				jsonRepresentation();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonRepresentationContext extends ParserRuleContext {
		public TerminalNode JSON() { return getToken(SqlBaseParser.JSON, 0); }
		public TerminalNode ENCODING() { return getToken(SqlBaseParser.ENCODING, 0); }
		public TerminalNode UTF8() { return getToken(SqlBaseParser.UTF8, 0); }
		public TerminalNode UTF16() { return getToken(SqlBaseParser.UTF16, 0); }
		public TerminalNode UTF32() { return getToken(SqlBaseParser.UTF32, 0); }
		public JsonRepresentationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonRepresentation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonRepresentation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonRepresentation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonRepresentation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonRepresentationContext jsonRepresentation() throws RecognitionException {
		JsonRepresentationContext _localctx = new JsonRepresentationContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_jsonRepresentation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2406);
			match(JSON);
			setState(2409);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ENCODING) {
				{
				setState(2407);
				match(ENCODING);
				setState(2408);
				_la = _input.LA(1);
				if ( !((((_la - 270)) & ~0x3f) == 0 && ((1L << (_la - 270)) & 7L) != 0) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonArgumentContext extends ParserRuleContext {
		public JsonValueExpressionContext jsonValueExpression() {
			return getRuleContext(JsonValueExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public JsonArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonArgumentContext jsonArgument() throws RecognitionException {
		JsonArgumentContext _localctx = new JsonArgumentContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_jsonArgument);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2411);
			jsonValueExpression();
			setState(2412);
			match(AS);
			setState(2413);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonExistsErrorBehaviorContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public TerminalNode UNKNOWN() { return getToken(SqlBaseParser.UNKNOWN, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public JsonExistsErrorBehaviorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonExistsErrorBehavior; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonExistsErrorBehavior(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonExistsErrorBehavior(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonExistsErrorBehavior(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonExistsErrorBehaviorContext jsonExistsErrorBehavior() throws RecognitionException {
		JsonExistsErrorBehaviorContext _localctx = new JsonExistsErrorBehaviorContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_jsonExistsErrorBehavior);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2415);
			_la = _input.LA(1);
			if ( !(_la==ERROR || _la==FALSE || _la==TRUE || _la==UNKNOWN) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonValueBehaviorContext extends ParserRuleContext {
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode DEFAULT() { return getToken(SqlBaseParser.DEFAULT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public JsonValueBehaviorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonValueBehavior; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonValueBehavior(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonValueBehavior(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonValueBehavior(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonValueBehaviorContext jsonValueBehavior() throws RecognitionException {
		JsonValueBehaviorContext _localctx = new JsonValueBehaviorContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_jsonValueBehavior);
		try {
			setState(2421);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ERROR:
				enterOuterAlt(_localctx, 1);
				{
				setState(2417);
				match(ERROR);
				}
				break;
			case NULL:
				enterOuterAlt(_localctx, 2);
				{
				setState(2418);
				match(NULL);
				}
				break;
			case DEFAULT:
				enterOuterAlt(_localctx, 3);
				{
				setState(2419);
				match(DEFAULT);
				setState(2420);
				expression();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonQueryWrapperBehaviorContext extends ParserRuleContext {
		public TerminalNode WITHOUT() { return getToken(SqlBaseParser.WITHOUT, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode CONDITIONAL() { return getToken(SqlBaseParser.CONDITIONAL, 0); }
		public TerminalNode UNCONDITIONAL() { return getToken(SqlBaseParser.UNCONDITIONAL, 0); }
		public JsonQueryWrapperBehaviorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonQueryWrapperBehavior; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonQueryWrapperBehavior(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonQueryWrapperBehavior(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonQueryWrapperBehavior(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonQueryWrapperBehaviorContext jsonQueryWrapperBehavior() throws RecognitionException {
		JsonQueryWrapperBehaviorContext _localctx = new JsonQueryWrapperBehaviorContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_jsonQueryWrapperBehavior);
		int _la;
		try {
			setState(2434);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case WITHOUT:
				enterOuterAlt(_localctx, 1);
				{
				setState(2423);
				match(WITHOUT);
				setState(2425);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ARRAY) {
					{
					setState(2424);
					match(ARRAY);
					}
				}

				}
				break;
			case WITH:
				enterOuterAlt(_localctx, 2);
				{
				setState(2427);
				match(WITH);
				setState(2429);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CONDITIONAL || _la==UNCONDITIONAL) {
					{
					setState(2428);
					_la = _input.LA(1);
					if ( !(_la==CONDITIONAL || _la==UNCONDITIONAL) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(2432);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ARRAY) {
					{
					setState(2431);
					match(ARRAY);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonQueryBehaviorContext extends ParserRuleContext {
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode OBJECT() { return getToken(SqlBaseParser.OBJECT, 0); }
		public JsonQueryBehaviorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonQueryBehavior; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonQueryBehavior(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonQueryBehavior(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonQueryBehavior(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonQueryBehaviorContext jsonQueryBehavior() throws RecognitionException {
		JsonQueryBehaviorContext _localctx = new JsonQueryBehaviorContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_jsonQueryBehavior);
		try {
			setState(2442);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,324,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2436);
				match(ERROR);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2437);
				match(NULL);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2438);
				match(EMPTY);
				setState(2439);
				match(ARRAY);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2440);
				match(EMPTY);
				setState(2441);
				match(OBJECT);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsonObjectMemberContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode VALUE() { return getToken(SqlBaseParser.VALUE, 0); }
		public JsonValueExpressionContext jsonValueExpression() {
			return getRuleContext(JsonValueExpressionContext.class,0);
		}
		public TerminalNode KEY() { return getToken(SqlBaseParser.KEY, 0); }
		public JsonObjectMemberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonObjectMember; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonObjectMember(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonObjectMember(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonObjectMember(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonObjectMemberContext jsonObjectMember() throws RecognitionException {
		JsonObjectMemberContext _localctx = new JsonObjectMemberContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_jsonObjectMember);
		try {
			setState(2455);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,326,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2445);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,325,_ctx) ) {
				case 1:
					{
					setState(2444);
					match(KEY);
					}
					break;
				}
				setState(2447);
				expression();
				setState(2448);
				match(VALUE);
				setState(2449);
				jsonValueExpression();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2451);
				expression();
				setState(2452);
				match(T__9);
				setState(2453);
				jsonValueExpression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ProcessingModeContext extends ParserRuleContext {
		public TerminalNode RUNNING() { return getToken(SqlBaseParser.RUNNING, 0); }
		public TerminalNode FINAL() { return getToken(SqlBaseParser.FINAL, 0); }
		public ProcessingModeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_processingMode; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterProcessingMode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitProcessingMode(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitProcessingMode(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ProcessingModeContext processingMode() throws RecognitionException {
		ProcessingModeContext _localctx = new ProcessingModeContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_processingMode);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2457);
			_la = _input.LA(1);
			if ( !(_la==FINAL || _la==RUNNING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NullTreatmentContext extends ParserRuleContext {
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode RESPECT() { return getToken(SqlBaseParser.RESPECT, 0); }
		public NullTreatmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nullTreatment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullTreatment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullTreatment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullTreatment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NullTreatmentContext nullTreatment() throws RecognitionException {
		NullTreatmentContext _localctx = new NullTreatmentContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_nullTreatment);
		try {
			setState(2463);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IGNORE:
				enterOuterAlt(_localctx, 1);
				{
				setState(2459);
				match(IGNORE);
				setState(2460);
				match(NULLS);
				}
				break;
			case RESPECT:
				enterOuterAlt(_localctx, 2);
				{
				setState(2461);
				match(RESPECT);
				setState(2462);
				match(NULLS);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StringContext extends ParserRuleContext {
		public StringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string; }
	 
		public StringContext() { }
		public void copyFrom(StringContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnicodeStringLiteralContext extends StringContext {
		public TerminalNode UNICODE_STRING() { return getToken(SqlBaseParser.UNICODE_STRING, 0); }
		public TerminalNode UESCAPE() { return getToken(SqlBaseParser.UESCAPE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public UnicodeStringLiteralContext(StringContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnicodeStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnicodeStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnicodeStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BasicStringLiteralContext extends StringContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public BasicStringLiteralContext(StringContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBasicStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBasicStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBasicStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StringContext string() throws RecognitionException {
		StringContext _localctx = new StringContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_string);
		try {
			setState(2471);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				_localctx = new BasicStringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2465);
				match(STRING);
				}
				break;
			case UNICODE_STRING:
				_localctx = new UnicodeStringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2466);
				match(UNICODE_STRING);
				setState(2469);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,328,_ctx) ) {
				case 1:
					{
					setState(2467);
					match(UESCAPE);
					setState(2468);
					match(STRING);
					}
					break;
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TimeZoneSpecifierContext extends ParserRuleContext {
		public TimeZoneSpecifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeZoneSpecifier; }
	 
		public TimeZoneSpecifierContext() { }
		public void copyFrom(TimeZoneSpecifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimeZoneIntervalContext extends TimeZoneSpecifierContext {
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public TimeZoneIntervalContext(TimeZoneSpecifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimeZoneInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimeZoneInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimeZoneInterval(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimeZoneStringContext extends TimeZoneSpecifierContext {
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TimeZoneStringContext(TimeZoneSpecifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimeZoneString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimeZoneString(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimeZoneString(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimeZoneSpecifierContext timeZoneSpecifier() throws RecognitionException {
		TimeZoneSpecifierContext _localctx = new TimeZoneSpecifierContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_timeZoneSpecifier);
		try {
			setState(2479);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,330,_ctx) ) {
			case 1:
				_localctx = new TimeZoneIntervalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2473);
				match(TIME);
				setState(2474);
				match(ZONE);
				setState(2475);
				interval();
				}
				break;
			case 2:
				_localctx = new TimeZoneStringContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2476);
				match(TIME);
				setState(2477);
				match(ZONE);
				setState(2478);
				string();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2481);
			_la = _input.LA(1);
			if ( !((((_la - 290)) & ~0x3f) == 0 && ((1L << (_la - 290)) & 63L) != 0) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonQuantifierContext extends ParserRuleContext {
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public ComparisonQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonQuantifierContext comparisonQuantifier() throws RecognitionException {
		ComparisonQuantifierContext _localctx = new ComparisonQuantifierContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_comparisonQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2483);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==ANY || _la==SOME) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2485);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntervalContext extends ParserRuleContext {
		public Token sign;
		public IntervalFieldContext from;
		public IntervalFieldContext to;
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_interval);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2487);
			match(INTERVAL);
			setState(2489);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PLUS || _la==MINUS) {
				{
				setState(2488);
				((IntervalContext)_localctx).sign = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((IntervalContext)_localctx).sign = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(2491);
			string();
			setState(2492);
			((IntervalContext)_localctx).from = intervalField();
			setState(2495);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,332,_ctx) ) {
			case 1:
				{
				setState(2493);
				match(TO);
				setState(2494);
				((IntervalContext)_localctx).to = intervalField();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntervalFieldContext extends ParserRuleContext {
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalFieldContext intervalField() throws RecognitionException {
		IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_intervalField);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2497);
			_la = _input.LA(1);
			if ( !(_la==DAY || _la==HOUR || _la==MINUTE || _la==MONTH || _la==SECOND || _la==YEAR) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NormalFormContext extends ParserRuleContext {
		public TerminalNode NFD() { return getToken(SqlBaseParser.NFD, 0); }
		public TerminalNode NFC() { return getToken(SqlBaseParser.NFC, 0); }
		public TerminalNode NFKD() { return getToken(SqlBaseParser.NFKD, 0); }
		public TerminalNode NFKC() { return getToken(SqlBaseParser.NFKC, 0); }
		public NormalFormContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_normalForm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNormalForm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNormalForm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNormalForm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NormalFormContext normalForm() throws RecognitionException {
		NormalFormContext _localctx = new NormalFormContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_normalForm);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2499);
			_la = _input.LA(1);
			if ( !((((_la - 160)) & ~0x3f) == 0 && ((1L << (_la - 160)) & 15L) != 0) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TypeContext extends ParserRuleContext {
		public TypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type; }
	 
		public TypeContext() { }
		public void copyFrom(TypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowTypeContext extends TypeContext {
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public List<RowFieldContext> rowField() {
			return getRuleContexts(RowFieldContext.class);
		}
		public RowFieldContext rowField(int i) {
			return getRuleContext(RowFieldContext.class,i);
		}
		public RowTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntervalTypeContext extends TypeContext {
		public IntervalFieldContext from;
		public IntervalFieldContext to;
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IntervalTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArrayTypeContext extends TypeContext {
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public ArrayTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArrayType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArrayType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArrayType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoublePrecisionTypeContext extends TypeContext {
		public TerminalNode DOUBLE() { return getToken(SqlBaseParser.DOUBLE, 0); }
		public TerminalNode PRECISION() { return getToken(SqlBaseParser.PRECISION, 0); }
		public DoublePrecisionTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDoublePrecisionType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDoublePrecisionType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDoublePrecisionType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LegacyArrayTypeContext extends TypeContext {
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public LegacyArrayTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLegacyArrayType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLegacyArrayType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLegacyArrayType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GenericTypeContext extends TypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TypeParameterContext> typeParameter() {
			return getRuleContexts(TypeParameterContext.class);
		}
		public TypeParameterContext typeParameter(int i) {
			return getRuleContext(TypeParameterContext.class,i);
		}
		public GenericTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGenericType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGenericType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGenericType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DateTimeTypeContext extends TypeContext {
		public Token base;
		public TypeParameterContext precision;
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode WITHOUT() { return getToken(SqlBaseParser.WITHOUT, 0); }
		public List<TerminalNode> TIME() { return getTokens(SqlBaseParser.TIME); }
		public TerminalNode TIME(int i) {
			return getToken(SqlBaseParser.TIME, i);
		}
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public TypeParameterContext typeParameter() {
			return getRuleContext(TypeParameterContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public DateTimeTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDateTimeType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDateTimeType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDateTimeType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LegacyMapTypeContext extends TypeContext {
		public TypeContext keyType;
		public TypeContext valueType;
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public List<TypeContext> type() {
			return getRuleContexts(TypeContext.class);
		}
		public TypeContext type(int i) {
			return getRuleContext(TypeContext.class,i);
		}
		public LegacyMapTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLegacyMapType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLegacyMapType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLegacyMapType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeContext type() throws RecognitionException {
		return type(0);
	}

	private TypeContext type(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		TypeContext _localctx = new TypeContext(_ctx, _parentState);
		TypeContext _prevctx = _localctx;
		int _startState = 164;
		enterRecursionRule(_localctx, 164, RULE_type, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2592);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,343,_ctx) ) {
			case 1:
				{
				_localctx = new RowTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2502);
				match(ROW);
				setState(2503);
				match(T__1);
				setState(2504);
				rowField();
				setState(2509);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2505);
					match(T__3);
					setState(2506);
					rowField();
					}
					}
					setState(2511);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2512);
				match(T__2);
				}
				break;
			case 2:
				{
				_localctx = new IntervalTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2514);
				match(INTERVAL);
				setState(2515);
				((IntervalTypeContext)_localctx).from = intervalField();
				setState(2518);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,334,_ctx) ) {
				case 1:
					{
					setState(2516);
					match(TO);
					setState(2517);
					((IntervalTypeContext)_localctx).to = intervalField();
					}
					break;
				}
				}
				break;
			case 3:
				{
				_localctx = new DateTimeTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2520);
				((DateTimeTypeContext)_localctx).base = match(TIMESTAMP);
				setState(2525);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,335,_ctx) ) {
				case 1:
					{
					setState(2521);
					match(T__1);
					setState(2522);
					((DateTimeTypeContext)_localctx).precision = typeParameter();
					setState(2523);
					match(T__2);
					}
					break;
				}
				setState(2530);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,336,_ctx) ) {
				case 1:
					{
					setState(2527);
					match(WITHOUT);
					setState(2528);
					match(TIME);
					setState(2529);
					match(ZONE);
					}
					break;
				}
				}
				break;
			case 4:
				{
				_localctx = new DateTimeTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2532);
				((DateTimeTypeContext)_localctx).base = match(TIMESTAMP);
				setState(2537);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(2533);
					match(T__1);
					setState(2534);
					((DateTimeTypeContext)_localctx).precision = typeParameter();
					setState(2535);
					match(T__2);
					}
				}

				setState(2539);
				match(WITH);
				setState(2540);
				match(TIME);
				setState(2541);
				match(ZONE);
				}
				break;
			case 5:
				{
				_localctx = new DateTimeTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2542);
				((DateTimeTypeContext)_localctx).base = match(TIME);
				setState(2547);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,338,_ctx) ) {
				case 1:
					{
					setState(2543);
					match(T__1);
					setState(2544);
					((DateTimeTypeContext)_localctx).precision = typeParameter();
					setState(2545);
					match(T__2);
					}
					break;
				}
				setState(2552);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,339,_ctx) ) {
				case 1:
					{
					setState(2549);
					match(WITHOUT);
					setState(2550);
					match(TIME);
					setState(2551);
					match(ZONE);
					}
					break;
				}
				}
				break;
			case 6:
				{
				_localctx = new DateTimeTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2554);
				((DateTimeTypeContext)_localctx).base = match(TIME);
				setState(2559);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(2555);
					match(T__1);
					setState(2556);
					((DateTimeTypeContext)_localctx).precision = typeParameter();
					setState(2557);
					match(T__2);
					}
				}

				setState(2561);
				match(WITH);
				setState(2562);
				match(TIME);
				setState(2563);
				match(ZONE);
				}
				break;
			case 7:
				{
				_localctx = new DoublePrecisionTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2564);
				match(DOUBLE);
				setState(2565);
				match(PRECISION);
				}
				break;
			case 8:
				{
				_localctx = new LegacyArrayTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2566);
				match(ARRAY);
				setState(2567);
				match(LT);
				setState(2568);
				type(0);
				setState(2569);
				match(GT);
				}
				break;
			case 9:
				{
				_localctx = new LegacyMapTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2571);
				match(MAP);
				setState(2572);
				match(LT);
				setState(2573);
				((LegacyMapTypeContext)_localctx).keyType = type(0);
				setState(2574);
				match(T__3);
				setState(2575);
				((LegacyMapTypeContext)_localctx).valueType = type(0);
				setState(2576);
				match(GT);
				}
				break;
			case 10:
				{
				_localctx = new GenericTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2578);
				identifier();
				setState(2590);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,342,_ctx) ) {
				case 1:
					{
					setState(2579);
					match(T__1);
					setState(2580);
					typeParameter();
					setState(2585);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2581);
						match(T__3);
						setState(2582);
						typeParameter();
						}
						}
						setState(2587);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2588);
					match(T__2);
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2603);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,345,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new ArrayTypeContext(new TypeContext(_parentctx, _parentState));
					pushNewRecursionContext(_localctx, _startState, RULE_type);
					setState(2594);
					if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
					setState(2595);
					match(ARRAY);
					setState(2599);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,344,_ctx) ) {
					case 1:
						{
						setState(2596);
						match(T__7);
						setState(2597);
						match(INTEGER_VALUE);
						setState(2598);
						match(T__8);
						}
						break;
					}
					}
					} 
				}
				setState(2605);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,345,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RowFieldContext extends ParserRuleContext {
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RowFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowFieldContext rowField() throws RecognitionException {
		RowFieldContext _localctx = new RowFieldContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_rowField);
		try {
			setState(2610);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,346,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2606);
				type(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2607);
				identifier();
				setState(2608);
				type(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TypeParameterContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TypeParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typeParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeParameterContext typeParameter() throws RecognitionException {
		TypeParameterContext _localctx = new TypeParameterContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_typeParameter);
		try {
			setState(2614);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(2612);
				match(INTEGER_VALUE);
				}
				break;
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DISTRIBUTED:
			case DOUBLE:
			case EMPTY:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERMUTE:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(2613);
				type(0);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2616);
			match(WHEN);
			setState(2617);
			((WhenClauseContext)_localctx).condition = expression();
			setState(2618);
			match(THEN);
			setState(2619);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FilterContext extends ParserRuleContext {
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public FilterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_filter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFilter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFilter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFilter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FilterContext filter() throws RecognitionException {
		FilterContext _localctx = new FilterContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_filter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2621);
			match(FILTER);
			setState(2622);
			match(T__1);
			setState(2623);
			match(WHERE);
			setState(2624);
			booleanExpression(0);
			setState(2625);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MergeCaseContext extends ParserRuleContext {
		public MergeCaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mergeCase; }
	 
		public MergeCaseContext() { }
		public void copyFrom(MergeCaseContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MergeInsertContext extends MergeCaseContext {
		public ExpressionContext condition;
		public IdentifierContext identifier;
		public List<IdentifierContext> targets = new ArrayList<IdentifierContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> values = new ArrayList<ExpressionContext>();
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public MergeInsertContext(MergeCaseContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMergeInsert(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMergeInsert(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMergeInsert(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MergeUpdateContext extends MergeCaseContext {
		public ExpressionContext condition;
		public IdentifierContext identifier;
		public List<IdentifierContext> targets = new ArrayList<IdentifierContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> values = new ArrayList<ExpressionContext>();
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public List<TerminalNode> EQ() { return getTokens(SqlBaseParser.EQ); }
		public TerminalNode EQ(int i) {
			return getToken(SqlBaseParser.EQ, i);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public MergeUpdateContext(MergeCaseContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMergeUpdate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMergeUpdate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMergeUpdate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MergeDeleteContext extends MergeCaseContext {
		public ExpressionContext condition;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public MergeDeleteContext(MergeCaseContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMergeDelete(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMergeDelete(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMergeDelete(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MergeCaseContext mergeCase() throws RecognitionException {
		MergeCaseContext _localctx = new MergeCaseContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_mergeCase);
		int _la;
		try {
			setState(2691);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,355,_ctx) ) {
			case 1:
				_localctx = new MergeUpdateContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2627);
				match(WHEN);
				setState(2628);
				match(MATCHED);
				setState(2631);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AND) {
					{
					setState(2629);
					match(AND);
					setState(2630);
					((MergeUpdateContext)_localctx).condition = expression();
					}
				}

				setState(2633);
				match(THEN);
				setState(2634);
				match(UPDATE);
				setState(2635);
				match(SET);
				setState(2636);
				((MergeUpdateContext)_localctx).identifier = identifier();
				((MergeUpdateContext)_localctx).targets.add(((MergeUpdateContext)_localctx).identifier);
				setState(2637);
				match(EQ);
				setState(2638);
				((MergeUpdateContext)_localctx).expression = expression();
				((MergeUpdateContext)_localctx).values.add(((MergeUpdateContext)_localctx).expression);
				setState(2646);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2639);
					match(T__3);
					setState(2640);
					((MergeUpdateContext)_localctx).identifier = identifier();
					((MergeUpdateContext)_localctx).targets.add(((MergeUpdateContext)_localctx).identifier);
					setState(2641);
					match(EQ);
					setState(2642);
					((MergeUpdateContext)_localctx).expression = expression();
					((MergeUpdateContext)_localctx).values.add(((MergeUpdateContext)_localctx).expression);
					}
					}
					setState(2648);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				_localctx = new MergeDeleteContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2649);
				match(WHEN);
				setState(2650);
				match(MATCHED);
				setState(2653);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AND) {
					{
					setState(2651);
					match(AND);
					setState(2652);
					((MergeDeleteContext)_localctx).condition = expression();
					}
				}

				setState(2655);
				match(THEN);
				setState(2656);
				match(DELETE);
				}
				break;
			case 3:
				_localctx = new MergeInsertContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2657);
				match(WHEN);
				setState(2658);
				match(NOT);
				setState(2659);
				match(MATCHED);
				setState(2662);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AND) {
					{
					setState(2660);
					match(AND);
					setState(2661);
					((MergeInsertContext)_localctx).condition = expression();
					}
				}

				setState(2664);
				match(THEN);
				setState(2665);
				match(INSERT);
				setState(2677);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(2666);
					match(T__1);
					setState(2667);
					((MergeInsertContext)_localctx).identifier = identifier();
					((MergeInsertContext)_localctx).targets.add(((MergeInsertContext)_localctx).identifier);
					setState(2672);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2668);
						match(T__3);
						setState(2669);
						((MergeInsertContext)_localctx).identifier = identifier();
						((MergeInsertContext)_localctx).targets.add(((MergeInsertContext)_localctx).identifier);
						}
						}
						setState(2674);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2675);
					match(T__2);
					}
				}

				setState(2679);
				match(VALUES);
				setState(2680);
				match(T__1);
				setState(2681);
				((MergeInsertContext)_localctx).expression = expression();
				((MergeInsertContext)_localctx).values.add(((MergeInsertContext)_localctx).expression);
				setState(2686);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2682);
					match(T__3);
					setState(2683);
					((MergeInsertContext)_localctx).expression = expression();
					((MergeInsertContext)_localctx).values.add(((MergeInsertContext)_localctx).expression);
					}
					}
					setState(2688);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2689);
				match(T__2);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OverContext extends ParserRuleContext {
		public IdentifierContext windowName;
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public WindowSpecificationContext windowSpecification() {
			return getRuleContext(WindowSpecificationContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public OverContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_over; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOver(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOver(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOver(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OverContext over() throws RecognitionException {
		OverContext _localctx = new OverContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_over);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2693);
			match(OVER);
			setState(2699);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DISTRIBUTED:
			case DOUBLE:
			case EMPTY:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERMUTE:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				{
				setState(2694);
				((OverContext)_localctx).windowName = identifier();
				}
				break;
			case T__1:
				{
				setState(2695);
				match(T__1);
				setState(2696);
				windowSpecification();
				setState(2697);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowFrameContext extends ParserRuleContext {
		public FrameExtentContext frameExtent() {
			return getRuleContext(FrameExtentContext.class,0);
		}
		public TerminalNode MEASURES() { return getToken(SqlBaseParser.MEASURES, 0); }
		public List<MeasureDefinitionContext> measureDefinition() {
			return getRuleContexts(MeasureDefinitionContext.class);
		}
		public MeasureDefinitionContext measureDefinition(int i) {
			return getRuleContext(MeasureDefinitionContext.class,i);
		}
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
		public SkipToContext skipTo() {
			return getRuleContext(SkipToContext.class,0);
		}
		public TerminalNode PATTERN() { return getToken(SqlBaseParser.PATTERN, 0); }
		public RowPatternContext rowPattern() {
			return getRuleContext(RowPatternContext.class,0);
		}
		public TerminalNode SUBSET() { return getToken(SqlBaseParser.SUBSET, 0); }
		public List<SubsetDefinitionContext> subsetDefinition() {
			return getRuleContexts(SubsetDefinitionContext.class);
		}
		public SubsetDefinitionContext subsetDefinition(int i) {
			return getRuleContext(SubsetDefinitionContext.class,i);
		}
		public TerminalNode DEFINE() { return getToken(SqlBaseParser.DEFINE, 0); }
		public List<VariableDefinitionContext> variableDefinition() {
			return getRuleContexts(VariableDefinitionContext.class);
		}
		public VariableDefinitionContext variableDefinition(int i) {
			return getRuleContext(VariableDefinitionContext.class,i);
		}
		public TerminalNode INITIAL() { return getToken(SqlBaseParser.INITIAL, 0); }
		public TerminalNode SEEK() { return getToken(SqlBaseParser.SEEK, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowFrame(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_windowFrame);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2710);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==MEASURES) {
				{
				setState(2701);
				match(MEASURES);
				setState(2702);
				measureDefinition();
				setState(2707);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2703);
					match(T__3);
					setState(2704);
					measureDefinition();
					}
					}
					setState(2709);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(2712);
			frameExtent();
			setState(2716);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AFTER) {
				{
				setState(2713);
				match(AFTER);
				setState(2714);
				match(MATCH);
				setState(2715);
				skipTo();
				}
			}

			setState(2719);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INITIAL || _la==SEEK) {
				{
				setState(2718);
				_la = _input.LA(1);
				if ( !(_la==INITIAL || _la==SEEK) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(2726);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PATTERN) {
				{
				setState(2721);
				match(PATTERN);
				setState(2722);
				match(T__1);
				setState(2723);
				rowPattern(0);
				setState(2724);
				match(T__2);
				}
			}

			setState(2737);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SUBSET) {
				{
				setState(2728);
				match(SUBSET);
				setState(2729);
				subsetDefinition();
				setState(2734);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2730);
					match(T__3);
					setState(2731);
					subsetDefinition();
					}
					}
					setState(2736);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(2748);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFINE) {
				{
				setState(2739);
				match(DEFINE);
				setState(2740);
				variableDefinition();
				setState(2745);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2741);
					match(T__3);
					setState(2742);
					variableDefinition();
					}
					}
					setState(2747);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FrameExtentContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode GROUPS() { return getToken(SqlBaseParser.GROUPS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public FrameExtentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameExtent; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFrameExtent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFrameExtent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFrameExtent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameExtentContext frameExtent() throws RecognitionException {
		FrameExtentContext _localctx = new FrameExtentContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_frameExtent);
		try {
			setState(2774);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,366,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2750);
				((FrameExtentContext)_localctx).frameType = match(RANGE);
				setState(2751);
				((FrameExtentContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2752);
				((FrameExtentContext)_localctx).frameType = match(ROWS);
				setState(2753);
				((FrameExtentContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2754);
				((FrameExtentContext)_localctx).frameType = match(GROUPS);
				setState(2755);
				((FrameExtentContext)_localctx).start = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2756);
				((FrameExtentContext)_localctx).frameType = match(RANGE);
				setState(2757);
				match(BETWEEN);
				setState(2758);
				((FrameExtentContext)_localctx).start = frameBound();
				setState(2759);
				match(AND);
				setState(2760);
				((FrameExtentContext)_localctx).end = frameBound();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(2762);
				((FrameExtentContext)_localctx).frameType = match(ROWS);
				setState(2763);
				match(BETWEEN);
				setState(2764);
				((FrameExtentContext)_localctx).start = frameBound();
				setState(2765);
				match(AND);
				setState(2766);
				((FrameExtentContext)_localctx).end = frameBound();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(2768);
				((FrameExtentContext)_localctx).frameType = match(GROUPS);
				setState(2769);
				match(BETWEEN);
				setState(2770);
				((FrameExtentContext)_localctx).start = frameBound();
				setState(2771);
				match(AND);
				setState(2772);
				((FrameExtentContext)_localctx).end = frameBound();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FrameBoundContext extends ParserRuleContext {
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
	 
		public FrameBoundContext() { }
		public void copyFrom(FrameBoundContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BoundedFrameContext extends FrameBoundContext {
		public Token boundType;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public BoundedFrameContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBoundedFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBoundedFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBoundedFrame(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnboundedFrameContext extends FrameBoundContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public UnboundedFrameContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnboundedFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnboundedFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnboundedFrame(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentRowBoundContext extends FrameBoundContext {
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public CurrentRowBoundContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentRowBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentRowBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentRowBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_frameBound);
		int _la;
		try {
			setState(2785);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,367,_ctx) ) {
			case 1:
				_localctx = new UnboundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2776);
				match(UNBOUNDED);
				setState(2777);
				((UnboundedFrameContext)_localctx).boundType = match(PRECEDING);
				}
				break;
			case 2:
				_localctx = new UnboundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2778);
				match(UNBOUNDED);
				setState(2779);
				((UnboundedFrameContext)_localctx).boundType = match(FOLLOWING);
				}
				break;
			case 3:
				_localctx = new CurrentRowBoundContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2780);
				match(CURRENT);
				setState(2781);
				match(ROW);
				}
				break;
			case 4:
				_localctx = new BoundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2782);
				expression();
				setState(2783);
				((BoundedFrameContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FOLLOWING || _la==PRECEDING) ) {
					((BoundedFrameContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RowPatternContext extends ParserRuleContext {
		public RowPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowPattern; }
	 
		public RowPatternContext() { }
		public void copyFrom(RowPatternContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuantifiedPrimaryContext extends RowPatternContext {
		public PatternPrimaryContext patternPrimary() {
			return getRuleContext(PatternPrimaryContext.class,0);
		}
		public PatternQuantifierContext patternQuantifier() {
			return getRuleContext(PatternQuantifierContext.class,0);
		}
		public QuantifiedPrimaryContext(RowPatternContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuantifiedPrimary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuantifiedPrimary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuantifiedPrimary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PatternConcatenationContext extends RowPatternContext {
		public List<RowPatternContext> rowPattern() {
			return getRuleContexts(RowPatternContext.class);
		}
		public RowPatternContext rowPattern(int i) {
			return getRuleContext(RowPatternContext.class,i);
		}
		public PatternConcatenationContext(RowPatternContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternConcatenation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternConcatenation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternConcatenation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PatternAlternationContext extends RowPatternContext {
		public List<RowPatternContext> rowPattern() {
			return getRuleContexts(RowPatternContext.class);
		}
		public RowPatternContext rowPattern(int i) {
			return getRuleContext(RowPatternContext.class,i);
		}
		public PatternAlternationContext(RowPatternContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternAlternation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternAlternation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternAlternation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowPatternContext rowPattern() throws RecognitionException {
		return rowPattern(0);
	}

	private RowPatternContext rowPattern(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		RowPatternContext _localctx = new RowPatternContext(_ctx, _parentState);
		RowPatternContext _prevctx = _localctx;
		int _startState = 184;
		enterRecursionRule(_localctx, 184, RULE_rowPattern, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QuantifiedPrimaryContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(2788);
			patternPrimary();
			setState(2790);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,368,_ctx) ) {
			case 1:
				{
				setState(2789);
				patternQuantifier();
				}
				break;
			}
			}
			_ctx.stop = _input.LT(-1);
			setState(2799);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,370,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2797);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,369,_ctx) ) {
					case 1:
						{
						_localctx = new PatternConcatenationContext(new RowPatternContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_rowPattern);
						setState(2792);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2793);
						rowPattern(3);
						}
						break;
					case 2:
						{
						_localctx = new PatternAlternationContext(new RowPatternContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_rowPattern);
						setState(2794);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2795);
						match(T__10);
						setState(2796);
						rowPattern(2);
						}
						break;
					}
					} 
				}
				setState(2801);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,370,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PatternPrimaryContext extends ParserRuleContext {
		public PatternPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_patternPrimary; }
	 
		public PatternPrimaryContext() { }
		public void copyFrom(PatternPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PatternPermutationContext extends PatternPrimaryContext {
		public TerminalNode PERMUTE() { return getToken(SqlBaseParser.PERMUTE, 0); }
		public List<RowPatternContext> rowPattern() {
			return getRuleContexts(RowPatternContext.class);
		}
		public RowPatternContext rowPattern(int i) {
			return getRuleContext(RowPatternContext.class,i);
		}
		public PatternPermutationContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternPermutation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternPermutation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternPermutation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PartitionEndAnchorContext extends PatternPrimaryContext {
		public PartitionEndAnchorContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionEndAnchor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionEndAnchor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionEndAnchor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PatternVariableContext extends PatternPrimaryContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public PatternVariableContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternVariable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternVariable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternVariable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExcludedPatternContext extends PatternPrimaryContext {
		public RowPatternContext rowPattern() {
			return getRuleContext(RowPatternContext.class,0);
		}
		public ExcludedPatternContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExcludedPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExcludedPattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExcludedPattern(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PartitionStartAnchorContext extends PatternPrimaryContext {
		public PartitionStartAnchorContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionStartAnchor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionStartAnchor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionStartAnchor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class EmptyPatternContext extends PatternPrimaryContext {
		public EmptyPatternContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterEmptyPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitEmptyPattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitEmptyPattern(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GroupedPatternContext extends PatternPrimaryContext {
		public RowPatternContext rowPattern() {
			return getRuleContext(RowPatternContext.class,0);
		}
		public GroupedPatternContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupedPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupedPattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupedPattern(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PatternPrimaryContext patternPrimary() throws RecognitionException {
		PatternPrimaryContext _localctx = new PatternPrimaryContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_patternPrimary);
		int _la;
		try {
			setState(2827);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,372,_ctx) ) {
			case 1:
				_localctx = new PatternVariableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2802);
				identifier();
				}
				break;
			case 2:
				_localctx = new EmptyPatternContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2803);
				match(T__1);
				setState(2804);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new PatternPermutationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2805);
				match(PERMUTE);
				setState(2806);
				match(T__1);
				setState(2807);
				rowPattern(0);
				setState(2812);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2808);
					match(T__3);
					setState(2809);
					rowPattern(0);
					}
					}
					setState(2814);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2815);
				match(T__2);
				}
				break;
			case 4:
				_localctx = new GroupedPatternContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2817);
				match(T__1);
				setState(2818);
				rowPattern(0);
				setState(2819);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new PartitionStartAnchorContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2821);
				match(T__11);
				}
				break;
			case 6:
				_localctx = new PartitionEndAnchorContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2822);
				match(T__12);
				}
				break;
			case 7:
				_localctx = new ExcludedPatternContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(2823);
				match(T__13);
				setState(2824);
				rowPattern(0);
				setState(2825);
				match(T__14);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PatternQuantifierContext extends ParserRuleContext {
		public PatternQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_patternQuantifier; }
	 
		public PatternQuantifierContext() { }
		public void copyFrom(PatternQuantifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ZeroOrMoreQuantifierContext extends PatternQuantifierContext {
		public Token reluctant;
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode QUESTION_MARK() { return getToken(SqlBaseParser.QUESTION_MARK, 0); }
		public ZeroOrMoreQuantifierContext(PatternQuantifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterZeroOrMoreQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitZeroOrMoreQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitZeroOrMoreQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class OneOrMoreQuantifierContext extends PatternQuantifierContext {
		public Token reluctant;
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode QUESTION_MARK() { return getToken(SqlBaseParser.QUESTION_MARK, 0); }
		public OneOrMoreQuantifierContext(PatternQuantifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOneOrMoreQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOneOrMoreQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOneOrMoreQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ZeroOrOneQuantifierContext extends PatternQuantifierContext {
		public Token reluctant;
		public List<TerminalNode> QUESTION_MARK() { return getTokens(SqlBaseParser.QUESTION_MARK); }
		public TerminalNode QUESTION_MARK(int i) {
			return getToken(SqlBaseParser.QUESTION_MARK, i);
		}
		public ZeroOrOneQuantifierContext(PatternQuantifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterZeroOrOneQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitZeroOrOneQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitZeroOrOneQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RangeQuantifierContext extends PatternQuantifierContext {
		public Token exactly;
		public Token reluctant;
		public Token atLeast;
		public Token atMost;
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public TerminalNode QUESTION_MARK() { return getToken(SqlBaseParser.QUESTION_MARK, 0); }
		public RangeQuantifierContext(PatternQuantifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRangeQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRangeQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRangeQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PatternQuantifierContext patternQuantifier() throws RecognitionException {
		PatternQuantifierContext _localctx = new PatternQuantifierContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_patternQuantifier);
		int _la;
		try {
			setState(2859);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,380,_ctx) ) {
			case 1:
				_localctx = new ZeroOrMoreQuantifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2829);
				match(ASTERISK);
				setState(2831);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,373,_ctx) ) {
				case 1:
					{
					setState(2830);
					((ZeroOrMoreQuantifierContext)_localctx).reluctant = match(QUESTION_MARK);
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new OneOrMoreQuantifierContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2833);
				match(PLUS);
				setState(2835);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,374,_ctx) ) {
				case 1:
					{
					setState(2834);
					((OneOrMoreQuantifierContext)_localctx).reluctant = match(QUESTION_MARK);
					}
					break;
				}
				}
				break;
			case 3:
				_localctx = new ZeroOrOneQuantifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2837);
				match(QUESTION_MARK);
				setState(2839);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,375,_ctx) ) {
				case 1:
					{
					setState(2838);
					((ZeroOrOneQuantifierContext)_localctx).reluctant = match(QUESTION_MARK);
					}
					break;
				}
				}
				break;
			case 4:
				_localctx = new RangeQuantifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2841);
				match(T__15);
				setState(2842);
				((RangeQuantifierContext)_localctx).exactly = match(INTEGER_VALUE);
				setState(2843);
				match(T__16);
				setState(2845);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,376,_ctx) ) {
				case 1:
					{
					setState(2844);
					((RangeQuantifierContext)_localctx).reluctant = match(QUESTION_MARK);
					}
					break;
				}
				}
				break;
			case 5:
				_localctx = new RangeQuantifierContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2847);
				match(T__15);
				setState(2849);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTEGER_VALUE) {
					{
					setState(2848);
					((RangeQuantifierContext)_localctx).atLeast = match(INTEGER_VALUE);
					}
				}

				setState(2851);
				match(T__3);
				setState(2853);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTEGER_VALUE) {
					{
					setState(2852);
					((RangeQuantifierContext)_localctx).atMost = match(INTEGER_VALUE);
					}
				}

				setState(2855);
				match(T__16);
				setState(2857);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,379,_ctx) ) {
				case 1:
					{
					setState(2856);
					((RangeQuantifierContext)_localctx).reluctant = match(QUESTION_MARK);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class UpdateAssignmentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public UpdateAssignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_updateAssignment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUpdateAssignment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUpdateAssignment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUpdateAssignment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UpdateAssignmentContext updateAssignment() throws RecognitionException {
		UpdateAssignmentContext _localctx = new UpdateAssignmentContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_updateAssignment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2861);
			identifier();
			setState(2862);
			match(EQ);
			setState(2863);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExplainOptionContext extends ParserRuleContext {
		public ExplainOptionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_explainOption; }
	 
		public ExplainOptionContext() { }
		public void copyFrom(ExplainOptionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainFormatContext extends ExplainOptionContext {
		public Token value;
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
		public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
		public TerminalNode JSON() { return getToken(SqlBaseParser.JSON, 0); }
		public ExplainFormatContext(ExplainOptionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplainFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplainFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplainFormat(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainTypeContext extends ExplainOptionContext {
		public Token value;
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode DISTRIBUTED() { return getToken(SqlBaseParser.DISTRIBUTED, 0); }
		public TerminalNode VALIDATE() { return getToken(SqlBaseParser.VALIDATE, 0); }
		public TerminalNode IO() { return getToken(SqlBaseParser.IO, 0); }
		public ExplainTypeContext(ExplainOptionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplainType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplainType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplainType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExplainOptionContext explainOption() throws RecognitionException {
		ExplainOptionContext _localctx = new ExplainOptionContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_explainOption);
		int _la;
		try {
			setState(2869);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FORMAT:
				_localctx = new ExplainFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2865);
				match(FORMAT);
				setState(2866);
				((ExplainFormatContext)_localctx).value = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==GRAPHVIZ || _la==JSON || _la==TEXT) ) {
					((ExplainFormatContext)_localctx).value = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case TYPE:
				_localctx = new ExplainTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2867);
				match(TYPE);
				setState(2868);
				((ExplainTypeContext)_localctx).value = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==DISTRIBUTED || _la==IO || _la==LOGICAL || _la==VALIDATE) ) {
					((ExplainTypeContext)_localctx).value = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TransactionModeContext extends ParserRuleContext {
		public TransactionModeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transactionMode; }
	 
		public TransactionModeContext() { }
		public void copyFrom(TransactionModeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TransactionAccessModeContext extends TransactionModeContext {
		public Token accessMode;
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode WRITE() { return getToken(SqlBaseParser.WRITE, 0); }
		public TransactionAccessModeContext(TransactionModeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTransactionAccessMode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTransactionAccessMode(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTransactionAccessMode(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IsolationLevelContext extends TransactionModeContext {
		public TerminalNode ISOLATION() { return getToken(SqlBaseParser.ISOLATION, 0); }
		public TerminalNode LEVEL() { return getToken(SqlBaseParser.LEVEL, 0); }
		public LevelOfIsolationContext levelOfIsolation() {
			return getRuleContext(LevelOfIsolationContext.class,0);
		}
		public IsolationLevelContext(TransactionModeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIsolationLevel(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIsolationLevel(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIsolationLevel(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TransactionModeContext transactionMode() throws RecognitionException {
		TransactionModeContext _localctx = new TransactionModeContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_transactionMode);
		int _la;
		try {
			setState(2876);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ISOLATION:
				_localctx = new IsolationLevelContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2871);
				match(ISOLATION);
				setState(2872);
				match(LEVEL);
				setState(2873);
				levelOfIsolation();
				}
				break;
			case READ:
				_localctx = new TransactionAccessModeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2874);
				match(READ);
				setState(2875);
				((TransactionAccessModeContext)_localctx).accessMode = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ONLY || _la==WRITE) ) {
					((TransactionAccessModeContext)_localctx).accessMode = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LevelOfIsolationContext extends ParserRuleContext {
		public LevelOfIsolationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_levelOfIsolation; }
	 
		public LevelOfIsolationContext() { }
		public void copyFrom(LevelOfIsolationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ReadUncommittedContext extends LevelOfIsolationContext {
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(SqlBaseParser.UNCOMMITTED, 0); }
		public ReadUncommittedContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReadUncommitted(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReadUncommitted(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReadUncommitted(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SerializableContext extends LevelOfIsolationContext {
		public TerminalNode SERIALIZABLE() { return getToken(SqlBaseParser.SERIALIZABLE, 0); }
		public SerializableContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSerializable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSerializable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSerializable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ReadCommittedContext extends LevelOfIsolationContext {
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode COMMITTED() { return getToken(SqlBaseParser.COMMITTED, 0); }
		public ReadCommittedContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReadCommitted(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReadCommitted(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReadCommitted(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RepeatableReadContext extends LevelOfIsolationContext {
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public RepeatableReadContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRepeatableRead(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRepeatableRead(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRepeatableRead(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LevelOfIsolationContext levelOfIsolation() throws RecognitionException {
		LevelOfIsolationContext _localctx = new LevelOfIsolationContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_levelOfIsolation);
		try {
			setState(2885);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,383,_ctx) ) {
			case 1:
				_localctx = new ReadUncommittedContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2878);
				match(READ);
				setState(2879);
				match(UNCOMMITTED);
				}
				break;
			case 2:
				_localctx = new ReadCommittedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2880);
				match(READ);
				setState(2881);
				match(COMMITTED);
				}
				break;
			case 3:
				_localctx = new RepeatableReadContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2882);
				match(REPEATABLE);
				setState(2883);
				match(READ);
				}
				break;
			case 4:
				_localctx = new SerializableContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2884);
				match(SERIALIZABLE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CallArgumentContext extends ParserRuleContext {
		public CallArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_callArgument; }
	 
		public CallArgumentContext() { }
		public void copyFrom(CallArgumentContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PositionalArgumentContext extends CallArgumentContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public PositionalArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPositionalArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPositionalArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPositionalArgument(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NamedArgumentContext extends CallArgumentContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public NamedArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CallArgumentContext callArgument() throws RecognitionException {
		CallArgumentContext _localctx = new CallArgumentContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_callArgument);
		try {
			setState(2892);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,384,_ctx) ) {
			case 1:
				_localctx = new PositionalArgumentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2887);
				expression();
				}
				break;
			case 2:
				_localctx = new NamedArgumentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2888);
				identifier();
				setState(2889);
				match(T__5);
				setState(2890);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathElementContext extends ParserRuleContext {
		public PathElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathElement; }
	 
		public PathElementContext() { }
		public void copyFrom(PathElementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QualifiedArgumentContext extends PathElementContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedArgumentContext(PathElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedArgument(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnqualifiedArgumentContext extends PathElementContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UnqualifiedArgumentContext(PathElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnqualifiedArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnqualifiedArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnqualifiedArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathElementContext pathElement() throws RecognitionException {
		PathElementContext _localctx = new PathElementContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_pathElement);
		try {
			setState(2899);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,385,_ctx) ) {
			case 1:
				_localctx = new QualifiedArgumentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2894);
				identifier();
				setState(2895);
				match(T__0);
				setState(2896);
				identifier();
				}
				break;
			case 2:
				_localctx = new UnqualifiedArgumentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2898);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathSpecificationContext extends ParserRuleContext {
		public List<PathElementContext> pathElement() {
			return getRuleContexts(PathElementContext.class);
		}
		public PathElementContext pathElement(int i) {
			return getRuleContext(PathElementContext.class,i);
		}
		public PathSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPathSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPathSpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPathSpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathSpecificationContext pathSpecification() throws RecognitionException {
		PathSpecificationContext _localctx = new PathSpecificationContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_pathSpecification);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2901);
			pathElement();
			setState(2906);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(2902);
				match(T__3);
				setState(2903);
				pathElement();
				}
				}
				setState(2908);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrivilegeContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public PrivilegeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_privilege; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrivilege(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrivilege(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrivilege(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrivilegeContext privilege() throws RecognitionException {
		PrivilegeContext _localctx = new PrivilegeContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_privilege);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2909);
			_la = _input.LA(1);
			if ( !(_la==CREATE || _la==DELETE || _la==INSERT || _la==SELECT || _la==UPDATE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2911);
			identifier();
			setState(2916);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,387,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2912);
					match(T__0);
					setState(2913);
					identifier();
					}
					} 
				}
				setState(2918);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,387,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryPeriodContext extends ParserRuleContext {
		public ValueExpressionContext end;
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public RangeTypeContext rangeType() {
			return getRuleContext(RangeTypeContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public QueryPeriodContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPeriod; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPeriod(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPeriod(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPeriod(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPeriodContext queryPeriod() throws RecognitionException {
		QueryPeriodContext _localctx = new QueryPeriodContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_queryPeriod);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2919);
			match(FOR);
			setState(2920);
			rangeType();
			setState(2921);
			match(AS);
			setState(2922);
			match(OF);
			setState(2923);
			((QueryPeriodContext)_localctx).end = valueExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RangeTypeContext extends ParserRuleContext {
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode VERSION() { return getToken(SqlBaseParser.VERSION, 0); }
		public RangeTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRangeType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRangeType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRangeType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeTypeContext rangeType() throws RecognitionException {
		RangeTypeContext _localctx = new RangeTypeContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_rangeType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2925);
			_la = _input.LA(1);
			if ( !(_la==TIMESTAMP || _la==VERSION) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GrantorContext extends ParserRuleContext {
		public GrantorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantor; }
	 
		public GrantorContext() { }
		public void copyFrom(GrantorContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentUserGrantorContext extends GrantorContext {
		public TerminalNode CURRENT_USER() { return getToken(SqlBaseParser.CURRENT_USER, 0); }
		public CurrentUserGrantorContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentUserGrantor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentUserGrantor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentUserGrantor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SpecifiedPrincipalContext extends GrantorContext {
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public SpecifiedPrincipalContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSpecifiedPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSpecifiedPrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSpecifiedPrincipal(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentRoleGrantorContext extends GrantorContext {
		public TerminalNode CURRENT_ROLE() { return getToken(SqlBaseParser.CURRENT_ROLE, 0); }
		public CurrentRoleGrantorContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentRoleGrantor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentRoleGrantor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentRoleGrantor(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GrantorContext grantor() throws RecognitionException {
		GrantorContext _localctx = new GrantorContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_grantor);
		try {
			setState(2930);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DISTRIBUTED:
			case DOUBLE:
			case EMPTY:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERMUTE:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				_localctx = new SpecifiedPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2927);
				principal();
				}
				break;
			case CURRENT_USER:
				_localctx = new CurrentUserGrantorContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2928);
				match(CURRENT_USER);
				}
				break;
			case CURRENT_ROLE:
				_localctx = new CurrentRoleGrantorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2929);
				match(CURRENT_ROLE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrincipalContext extends ParserRuleContext {
		public PrincipalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_principal; }
	 
		public PrincipalContext() { }
		public void copyFrom(PrincipalContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnspecifiedPrincipalContext extends PrincipalContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UnspecifiedPrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnspecifiedPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnspecifiedPrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnspecifiedPrincipal(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UserPrincipalContext extends PrincipalContext {
		public TerminalNode USER() { return getToken(SqlBaseParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UserPrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUserPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUserPrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUserPrincipal(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RolePrincipalContext extends PrincipalContext {
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RolePrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRolePrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRolePrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRolePrincipal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrincipalContext principal() throws RecognitionException {
		PrincipalContext _localctx = new PrincipalContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_principal);
		try {
			setState(2937);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,389,_ctx) ) {
			case 1:
				_localctx = new UnspecifiedPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2932);
				identifier();
				}
				break;
			case 2:
				_localctx = new UserPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2933);
				match(USER);
				setState(2934);
				identifier();
				}
				break;
			case 3:
				_localctx = new RolePrincipalContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2935);
				match(ROLE);
				setState(2936);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RolesContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public RolesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_roles; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRoles(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RolesContext roles() throws RecognitionException {
		RolesContext _localctx = new RolesContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_roles);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2939);
			identifier();
			setState(2944);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(2940);
				match(T__3);
				setState(2941);
				identifier();
				}
				}
				setState(2946);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierContext extends ParserRuleContext {
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	 
		public IdentifierContext() { }
		public void copyFrom(IdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BackQuotedIdentifierContext extends IdentifierContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public BackQuotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBackQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBackQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBackQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuotedIdentifierContext extends IdentifierContext {
		public TerminalNode QUOTED_IDENTIFIER() { return getToken(SqlBaseParser.QUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DigitIdentifierContext extends IdentifierContext {
		public TerminalNode DIGIT_IDENTIFIER() { return getToken(SqlBaseParser.DIGIT_IDENTIFIER, 0); }
		public DigitIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDigitIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDigitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDigitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnquotedIdentifierContext extends IdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_identifier);
		try {
			setState(2952);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2947);
				match(IDENTIFIER);
				}
				break;
			case QUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2948);
				match(QUOTED_IDENTIFIER);
				}
				break;
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DISTRIBUTED:
			case DOUBLE:
			case EMPTY:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTIONS:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERMUTE:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2949);
				nonReserved();
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new BackQuotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2950);
				match(BACKQUOTED_IDENTIFIER);
				}
				break;
			case DIGIT_IDENTIFIER:
				_localctx = new DigitIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2951);
				match(DIGIT_IDENTIFIER);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_VALUE() { return getToken(SqlBaseParser.DOUBLE_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_number);
		int _la;
		try {
			setState(2966);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,395,_ctx) ) {
			case 1:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2955);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2954);
					match(MINUS);
					}
				}

				setState(2957);
				match(DECIMAL_VALUE);
				}
				break;
			case 2:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2959);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2958);
					match(MINUS);
					}
				}

				setState(2961);
				match(DOUBLE_VALUE);
				}
				break;
			case 3:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2963);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2962);
					match(MINUS);
					}
				}

				setState(2965);
				match(INTEGER_VALUE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode ABSENT() { return getToken(SqlBaseParser.ABSENT, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public TerminalNode BERNOULLI() { return getToken(SqlBaseParser.BERNOULLI, 0); }
		public TerminalNode BOTH() { return getToken(SqlBaseParser.BOTH, 0); }
		public TerminalNode CALL() { return getToken(SqlBaseParser.CALL, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode COMMITTED() { return getToken(SqlBaseParser.COMMITTED, 0); }
		public TerminalNode CONDITIONAL() { return getToken(SqlBaseParser.CONDITIONAL, 0); }
		public TerminalNode COPARTITION() { return getToken(SqlBaseParser.COPARTITION, 0); }
		public TerminalNode COUNT() { return getToken(SqlBaseParser.COUNT, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode DATE() { return getToken(SqlBaseParser.DATE, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode DEFAULT() { return getToken(SqlBaseParser.DEFAULT, 0); }
		public TerminalNode DEFINE() { return getToken(SqlBaseParser.DEFINE, 0); }
		public TerminalNode DEFINER() { return getToken(SqlBaseParser.DEFINER, 0); }
		public TerminalNode DENY() { return getToken(SqlBaseParser.DENY, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIPTOR() { return getToken(SqlBaseParser.DESCRIPTOR, 0); }
		public TerminalNode DISTRIBUTED() { return getToken(SqlBaseParser.DISTRIBUTED, 0); }
		public TerminalNode DOUBLE() { return getToken(SqlBaseParser.DOUBLE, 0); }
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode ENCODING() { return getToken(SqlBaseParser.ENCODING, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode EXCLUDING() { return getToken(SqlBaseParser.EXCLUDING, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode FETCH() { return getToken(SqlBaseParser.FETCH, 0); }
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode FINAL() { return getToken(SqlBaseParser.FINAL, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode GRANTED() { return getToken(SqlBaseParser.GRANTED, 0); }
		public TerminalNode GRANTS() { return getToken(SqlBaseParser.GRANTS, 0); }
		public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
		public TerminalNode GROUPS() { return getToken(SqlBaseParser.GROUPS, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode INCLUDING() { return getToken(SqlBaseParser.INCLUDING, 0); }
		public TerminalNode INITIAL() { return getToken(SqlBaseParser.INITIAL, 0); }
		public TerminalNode INPUT() { return getToken(SqlBaseParser.INPUT, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode INVOKER() { return getToken(SqlBaseParser.INVOKER, 0); }
		public TerminalNode IO() { return getToken(SqlBaseParser.IO, 0); }
		public TerminalNode ISOLATION() { return getToken(SqlBaseParser.ISOLATION, 0); }
		public TerminalNode JSON() { return getToken(SqlBaseParser.JSON, 0); }
		public TerminalNode KEEP() { return getToken(SqlBaseParser.KEEP, 0); }
		public TerminalNode KEY() { return getToken(SqlBaseParser.KEY, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode LEADING() { return getToken(SqlBaseParser.LEADING, 0); }
		public TerminalNode LEVEL() { return getToken(SqlBaseParser.LEVEL, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode MATCHES() { return getToken(SqlBaseParser.MATCHES, 0); }
		public TerminalNode MATCH_RECOGNIZE() { return getToken(SqlBaseParser.MATCH_RECOGNIZE, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode MEASURES() { return getToken(SqlBaseParser.MEASURES, 0); }
		public TerminalNode MERGE() { return getToken(SqlBaseParser.MERGE, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode NEXT() { return getToken(SqlBaseParser.NEXT, 0); }
		public TerminalNode NFC() { return getToken(SqlBaseParser.NFC, 0); }
		public TerminalNode NFD() { return getToken(SqlBaseParser.NFD, 0); }
		public TerminalNode NFKC() { return getToken(SqlBaseParser.NFKC, 0); }
		public TerminalNode NFKD() { return getToken(SqlBaseParser.NFKD, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode NONE() { return getToken(SqlBaseParser.NONE, 0); }
		public TerminalNode NULLIF() { return getToken(SqlBaseParser.NULLIF, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OBJECT() { return getToken(SqlBaseParser.OBJECT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode OFFSET() { return getToken(SqlBaseParser.OFFSET, 0); }
		public TerminalNode OMIT() { return getToken(SqlBaseParser.OMIT, 0); }
		public TerminalNode ONE() { return getToken(SqlBaseParser.ONE, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode ORDINALITY() { return getToken(SqlBaseParser.ORDINALITY, 0); }
		public TerminalNode OUTPUT() { return getToken(SqlBaseParser.OUTPUT, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode OVERFLOW() { return getToken(SqlBaseParser.OVERFLOW, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode PASSING() { return getToken(SqlBaseParser.PASSING, 0); }
		public TerminalNode PAST() { return getToken(SqlBaseParser.PAST, 0); }
		public TerminalNode PATH() { return getToken(SqlBaseParser.PATH, 0); }
		public TerminalNode PATTERN() { return getToken(SqlBaseParser.PATTERN, 0); }
		public TerminalNode PER() { return getToken(SqlBaseParser.PER, 0); }
		public TerminalNode PERMUTE() { return getToken(SqlBaseParser.PERMUTE, 0); }
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode PRECISION() { return getToken(SqlBaseParser.PRECISION, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode PRUNE() { return getToken(SqlBaseParser.PRUNE, 0); }
		public TerminalNode QUOTES() { return getToken(SqlBaseParser.QUOTES, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode RESPECT() { return getToken(SqlBaseParser.RESPECT, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode RETURNING() { return getToken(SqlBaseParser.RETURNING, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode RUNNING() { return getToken(SqlBaseParser.RUNNING, 0); }
		public TerminalNode SCALAR() { return getToken(SqlBaseParser.SCALAR, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode SECURITY() { return getToken(SqlBaseParser.SECURITY, 0); }
		public TerminalNode SEEK() { return getToken(SqlBaseParser.SEEK, 0); }
		public TerminalNode SERIALIZABLE() { return getToken(SqlBaseParser.SERIALIZABLE, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode STATS() { return getToken(SqlBaseParser.STATS, 0); }
		public TerminalNode SUBSET() { return getToken(SqlBaseParser.SUBSET, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public TerminalNode SYSTEM() { return getToken(SqlBaseParser.SYSTEM, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
		public TerminalNode TEXT_STRING() { return getToken(SqlBaseParser.TEXT_STRING, 0); }
		public TerminalNode TIES() { return getToken(SqlBaseParser.TIES, 0); }
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TRAILING() { return getToken(SqlBaseParser.TRAILING, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(SqlBaseParser.UNCOMMITTED, 0); }
		public TerminalNode UNCONDITIONAL() { return getToken(SqlBaseParser.UNCONDITIONAL, 0); }
		public TerminalNode UNIQUE() { return getToken(SqlBaseParser.UNIQUE, 0); }
		public TerminalNode UNKNOWN() { return getToken(SqlBaseParser.UNKNOWN, 0); }
		public TerminalNode UNMATCHED() { return getToken(SqlBaseParser.UNMATCHED, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode USER() { return getToken(SqlBaseParser.USER, 0); }
		public TerminalNode UTF16() { return getToken(SqlBaseParser.UTF16, 0); }
		public TerminalNode UTF32() { return getToken(SqlBaseParser.UTF32, 0); }
		public TerminalNode UTF8() { return getToken(SqlBaseParser.UTF8, 0); }
		public TerminalNode VALIDATE() { return getToken(SqlBaseParser.VALIDATE, 0); }
		public TerminalNode VALUE() { return getToken(SqlBaseParser.VALUE, 0); }
		public TerminalNode VERBOSE() { return getToken(SqlBaseParser.VERBOSE, 0); }
		public TerminalNode VERSION() { return getToken(SqlBaseParser.VERSION, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public TerminalNode WITHIN() { return getToken(SqlBaseParser.WITHIN, 0); }
		public TerminalNode WITHOUT() { return getToken(SqlBaseParser.WITHOUT, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public TerminalNode WRAPPER() { return getToken(SqlBaseParser.WRAPPER, 0); }
		public TerminalNode WRITE() { return getToken(SqlBaseParser.WRITE, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2968);
			_la = _input.LA(1);
			if ( !(((_la) & ~0x3f) == 0 && ((1L << _la) & -9203388181402353664L) != 0 || (((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -5967880946630052421L) != 0 || (((_la - 133)) & ~0x3f) == 0 && ((1L << (_la - 133)) & -778514395575617L) != 0 || (((_la - 198)) & ~0x3f) == 0 && ((1L << (_la - 198)) & 8592722952413183935L) != 0 || (((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 266985335L) != 0) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 21:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 33:
			return relation_sempred((RelationContext)_localctx, predIndex);
		case 59:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 61:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 62:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		case 82:
			return type_sempred((TypeContext)_localctx, predIndex);
		case 92:
			return rowPattern_sempred((RowPatternContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 2);
		case 1:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean relation_sempred(RelationContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 3:
			return precpred(_ctx, 2);
		case 4:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 5:
			return precpred(_ctx, 3);
		case 6:
			return precpred(_ctx, 2);
		case 7:
			return precpred(_ctx, 1);
		case 8:
			return precpred(_ctx, 5);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 9:
			return precpred(_ctx, 24);
		case 10:
			return precpred(_ctx, 22);
		}
		return true;
	}
	private boolean type_sempred(TypeContext _localctx, int predIndex) {
		switch (predIndex) {
		case 11:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean rowPattern_sempred(RowPatternContext _localctx, int predIndex) {
		switch (predIndex) {
		case 12:
			return precpred(_ctx, 2);
		case 13:
			return precpred(_ctx, 1);
		}
		return true;
	}

	private static final String _serializedATNSegment0 =
		"\u0004\u0001\u013d\u0b9b\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
		"\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
		"\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
		"\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
		"\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
		"\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
		"\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
		"\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
		"\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
		"\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
		"\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
		"\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
		"\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
		",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
		"1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
		"6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
		";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
		"@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
		"E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
		"J\u0002K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007"+
		"O\u0002P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007"+
		"T\u0002U\u0007U\u0002V\u0007V\u0002W\u0007W\u0002X\u0007X\u0002Y\u0007"+
		"Y\u0002Z\u0007Z\u0002[\u0007[\u0002\\\u0007\\\u0002]\u0007]\u0002^\u0007"+
		"^\u0002_\u0007_\u0002`\u0007`\u0002a\u0007a\u0002b\u0007b\u0002c\u0007"+
		"c\u0002d\u0007d\u0002e\u0007e\u0002f\u0007f\u0002g\u0007g\u0002h\u0007"+
		"h\u0002i\u0007i\u0002j\u0007j\u0002k\u0007k\u0002l\u0007l\u0002m\u0007"+
		"m\u0002n\u0007n\u0002o\u0007o\u0001\u0000\u0001\u0000\u0001\u0000\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u00fd\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u0102\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0106\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u010d\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0112\b"+
		"\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0116\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u011c\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u0120\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0135\b\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u0139\b\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u013d\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0141\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u0149\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u014d\b"+
		"\u0005\u0001\u0005\u0003\u0005\u0150\b\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0157\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u015e\b\u0005\n"+
		"\u0005\f\u0005\u0161\t\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u0166\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u016a\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0170\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u0177\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u0180\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u018c\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u0195\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u019e\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u01a4\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u01af\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u01b7\b\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u01bf\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u01c6\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u01d0\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u01d7\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0005\u0005\u01f2\b\u0005\n\u0005\f\u0005\u01f5"+
		"\t\u0005\u0003\u0005\u01f7\b\u0005\u0001\u0005\u0003\u0005\u01fa\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u01fe\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u0204\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u0209\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u0210\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u0215\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u0219\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u0221\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u0227\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u022b\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u0239\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0241\b\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0254\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u026b\b\u0005\n\u0005"+
		"\f\u0005\u026e\t\u0005\u0003\u0005\u0270\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u027a\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u027e\b"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u0285\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0005\u0005\u028d\b\u0005\n\u0005\f\u0005\u0290\t\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0295\b\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u029a\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u029e\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u02a4\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0005\u0005\u02ab\b\u0005\n\u0005\f\u0005\u02ae\t\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u02b3\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u02b7\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u02be\b\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u02c2\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005"+
		"\u0005\u02c8\b\u0005\n\u0005\f\u0005\u02cb\t\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u02cf\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u02d3\b"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u02db\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0005\u0005\u02e1\b\u0005\n\u0005\f\u0005\u02e4\t\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u02e8\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u02ec\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u02f6\b\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0005\u0005\u02fb\b\u0005\n\u0005\f\u0005\u02fe"+
		"\t\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0302\b\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u0306\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u0310\b\u0005\u0001\u0005\u0003\u0005\u0313\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u031a\b\u0005\n"+
		"\u0005\f\u0005\u031d\t\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0321"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0327"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u033f"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0345"+
		"\b\u0005\u0003\u0005\u0347\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u034d\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u0353\b\u0005\u0003\u0005\u0355\b\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u035d\b\u0005\u0003\u0005\u035f\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u0365\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u036b\b\u0005\u0003\u0005\u036d\b"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u037c\b\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u0381\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u0388\b\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u0394\b\u0005\u0003\u0005\u0396\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u039e\b\u0005\u0003\u0005\u03a0\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0005\u0005\u03b0\b\u0005\n\u0005\f\u0005\u03b3\t\u0005\u0003\u0005"+
		"\u03b5\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u03b9\b\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u03bd\b\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005"+
		"\u0005\u03cd\b\u0005\n\u0005\f\u0005\u03d0\t\u0005\u0003\u0005\u03d2\b"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u03e2\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u03ea"+
		"\b\u0005\n\u0005\f\u0005\u03ed\t\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u03f1\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u03f7\b\u0005\u0001\u0005\u0003\u0005\u03fa\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0004\u0005\u0401\b\u0005\u000b"+
		"\u0005\f\u0005\u0402\u0003\u0005\u0405\b\u0005\u0001\u0006\u0003\u0006"+
		"\u0408\b\u0006\u0001\u0006\u0001\u0006\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u040e\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0005\u0007\u0413\b"+
		"\u0007\n\u0007\f\u0007\u0416\t\u0007\u0001\b\u0001\b\u0003\b\u041a\b\b"+
		"\u0001\t\u0001\t\u0001\t\u0001\t\u0003\t\u0420\b\t\u0001\t\u0001\t\u0003"+
		"\t\u0424\b\t\u0001\t\u0001\t\u0003\t\u0428\b\t\u0001\n\u0001\n\u0001\n"+
		"\u0001\n\u0003\n\u042e\b\n\u0001\u000b\u0001\u000b\u0001\u000b\u0001\u000b"+
		"\u0001\f\u0001\f\u0001\f\u0005\f\u0437\b\f\n\f\f\f\u043a\t\f\u0001\r\u0001"+
		"\r\u0001\r\u0001\r\u0001\u000e\u0001\u000e\u0003\u000e\u0442\b\u000e\u0001"+
		"\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001"+
		"\u0010\u0005\u0010\u044b\b\u0010\n\u0010\f\u0010\u044e\t\u0010\u0001\u0011"+
		"\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0012"+
		"\u0001\u0012\u0001\u0012\u0001\u0012\u0005\u0012\u045a\b\u0012\n\u0012"+
		"\f\u0012\u045d\t\u0012\u0003\u0012\u045f\b\u0012\u0001\u0012\u0001\u0012"+
		"\u0001\u0012\u0003\u0012\u0464\b\u0012\u0003\u0012\u0466\b\u0012\u0001"+
		"\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u046d"+
		"\b\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u0473"+
		"\b\u0012\u0003\u0012\u0475\b\u0012\u0001\u0013\u0001\u0013\u0003\u0013"+
		"\u0479\b\u0013\u0001\u0014\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0015"+
		"\u0001\u0015\u0001\u0015\u0001\u0015\u0003\u0015\u0483\b\u0015\u0001\u0015"+
		"\u0001\u0015\u0001\u0015\u0001\u0015\u0003\u0015\u0489\b\u0015\u0001\u0015"+
		"\u0005\u0015\u048c\b\u0015\n\u0015\f\u0015\u048f\t\u0015\u0001\u0016\u0001"+
		"\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0005"+
		"\u0016\u0498\b\u0016\n\u0016\f\u0016\u049b\t\u0016\u0001\u0016\u0001\u0016"+
		"\u0001\u0016\u0001\u0016\u0003\u0016\u04a1\b\u0016\u0001\u0017\u0001\u0017"+
		"\u0003\u0017\u04a5\b\u0017\u0001\u0017\u0001\u0017\u0003\u0017\u04a9\b"+
		"\u0017\u0001\u0018\u0001\u0018\u0003\u0018\u04ad\b\u0018\u0001\u0018\u0001"+
		"\u0018\u0001\u0018\u0005\u0018\u04b2\b\u0018\n\u0018\f\u0018\u04b5\t\u0018"+
		"\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0005\u0018\u04bb\b\u0018"+
		"\n\u0018\f\u0018\u04be\t\u0018\u0003\u0018\u04c0\b\u0018\u0001\u0018\u0001"+
		"\u0018\u0003\u0018\u04c4\b\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0003"+
		"\u0018\u04c9\b\u0018\u0001\u0018\u0001\u0018\u0003\u0018\u04cd\b\u0018"+
		"\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0005\u0018\u04d3\b\u0018"+
		"\n\u0018\f\u0018\u04d6\t\u0018\u0003\u0018\u04d8\b\u0018\u0001\u0019\u0003"+
		"\u0019\u04db\b\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0005\u0019\u04e0"+
		"\b\u0019\n\u0019\f\u0019\u04e3\t\u0019\u0001\u001a\u0001\u001a\u0001\u001a"+
		"\u0001\u001a\u0001\u001a\u0001\u001a\u0005\u001a\u04eb\b\u001a\n\u001a"+
		"\f\u001a\u04ee\t\u001a\u0003\u001a\u04f0\b\u001a\u0001\u001a\u0001\u001a"+
		"\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0005\u001a\u04f8\b\u001a"+
		"\n\u001a\f\u001a\u04fb\t\u001a\u0003\u001a\u04fd\b\u001a\u0001\u001a\u0001"+
		"\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0005"+
		"\u001a\u0506\b\u001a\n\u001a\f\u001a\u0509\t\u001a\u0001\u001a\u0001\u001a"+
		"\u0003\u001a\u050d\b\u001a\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b"+
		"\u0005\u001b\u0513\b\u001b\n\u001b\f\u001b\u0516\t\u001b\u0003\u001b\u0518"+
		"\b\u001b\u0001\u001b\u0001\u001b\u0003\u001b\u051c\b\u001b\u0001\u001c"+
		"\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001d"+
		"\u0003\u001d\u0525\b\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d"+
		"\u0001\u001d\u0005\u001d\u052c\b\u001d\n\u001d\f\u001d\u052f\t\u001d\u0003"+
		"\u001d\u0531\b\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001"+
		"\u001d\u0005\u001d\u0538\b\u001d\n\u001d\f\u001d\u053b\t\u001d\u0003\u001d"+
		"\u053d\b\u001d\u0001\u001d\u0003\u001d\u0540\b\u001d\u0001\u001e\u0001"+
		"\u001e\u0003\u001e\u0544\b\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0001"+
		"\u001e\u0001\u001e\u0001\u001f\u0001\u001f\u0001 \u0001 \u0003 \u054f"+
		"\b \u0001 \u0003 \u0552\b \u0001 \u0001 \u0001 \u0001 \u0001 \u0003 \u0559"+
		"\b \u0001 \u0003 \u055c\b \u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001"+
		"!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001"+
		"!\u0003!\u056f\b!\u0005!\u0571\b!\n!\f!\u0574\t!\u0001\"\u0003\"\u0577"+
		"\b\"\u0001\"\u0001\"\u0003\"\u057b\b\"\u0001\"\u0001\"\u0003\"\u057f\b"+
		"\"\u0001\"\u0001\"\u0003\"\u0583\b\"\u0003\"\u0585\b\"\u0001#\u0001#\u0001"+
		"#\u0001#\u0001#\u0001#\u0001#\u0005#\u058e\b#\n#\f#\u0591\t#\u0001#\u0001"+
		"#\u0003#\u0595\b#\u0001$\u0001$\u0001$\u0001$\u0001$\u0001$\u0001$\u0003"+
		"$\u059e\b$\u0001%\u0001%\u0001&\u0001&\u0001\'\u0001\'\u0001\'\u0003\'"+
		"\u05a7\b\'\u0001\'\u0003\'\u05aa\b\'\u0001(\u0001(\u0001(\u0001(\u0003"+
		"(\u05b0\b(\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0005"+
		")\u05ba\b)\n)\f)\u05bd\t)\u0003)\u05bf\b)\u0001)\u0001)\u0001)\u0001)"+
		"\u0001)\u0005)\u05c6\b)\n)\f)\u05c9\t)\u0003)\u05cb\b)\u0001)\u0001)\u0001"+
		")\u0001)\u0005)\u05d1\b)\n)\f)\u05d4\t)\u0003)\u05d6\b)\u0001)\u0003)"+
		"\u05d9\b)\u0001)\u0001)\u0001)\u0003)\u05de\b)\u0001)\u0003)\u05e1\b)"+
		"\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0005)\u05eb"+
		"\b)\n)\f)\u05ee\t)\u0003)\u05f0\b)\u0001)\u0001)\u0001)\u0001)\u0005)"+
		"\u05f6\b)\n)\f)\u05f9\t)\u0001)\u0001)\u0003)\u05fd\b)\u0001)\u0001)\u0003"+
		")\u0601\b)\u0003)\u0603\b)\u0003)\u0605\b)\u0001*\u0001*\u0001*\u0001"+
		"*\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0003"+
		"+\u0614\b+\u0003+\u0616\b+\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,\u0001"+
		",\u0001,\u0001,\u0003,\u0621\b,\u0001-\u0001-\u0001-\u0001-\u0001-\u0001"+
		"-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001"+
		"-\u0001-\u0001-\u0001-\u0003-\u0636\b-\u0001.\u0001.\u0001.\u0001.\u0001"+
		".\u0001.\u0005.\u063e\b.\n.\f.\u0641\t.\u0001.\u0001.\u0001/\u0001/\u0001"+
		"/\u0001/\u00010\u00010\u00030\u064b\b0\u00010\u00010\u00030\u064f\b0\u0003"+
		"0\u0651\b0\u00011\u00011\u00011\u00011\u00051\u0657\b1\n1\f1\u065a\t1"+
		"\u00011\u00011\u00012\u00012\u00032\u0660\b2\u00012\u00012\u00012\u0001"+
		"2\u00012\u00012\u00012\u00012\u00012\u00052\u066b\b2\n2\f2\u066e\t2\u0001"+
		"2\u00012\u00012\u00032\u0673\b2\u00012\u00012\u00012\u00012\u00012\u0001"+
		"2\u00012\u00012\u00012\u00012\u00012\u00012\u00012\u00012\u00032\u0683"+
		"\b2\u00013\u00013\u00013\u00013\u00013\u00053\u068a\b3\n3\f3\u068d\t3"+
		"\u00033\u068f\b3\u00013\u00013\u00013\u00013\u00053\u0695\b3\n3\f3\u0698"+
		"\t3\u00033\u069a\b3\u00013\u00013\u00014\u00014\u00014\u00034\u06a1\b"+
		"4\u00014\u00014\u00014\u00034\u06a6\b4\u00015\u00015\u00015\u00015\u0001"+
		"5\u00015\u00015\u00055\u06af\b5\n5\f5\u06b2\t5\u00035\u06b4\b5\u00015"+
		"\u00015\u00035\u06b8\b5\u00035\u06ba\b5\u00015\u00015\u00015\u00015\u0001"+
		"5\u00015\u00035\u06c2\b5\u00015\u00015\u00015\u00015\u00015\u00015\u0005"+
		"5\u06ca\b5\n5\f5\u06cd\t5\u00015\u00015\u00015\u00035\u06d2\b5\u00035"+
		"\u06d4\b5\u00016\u00016\u00016\u00016\u00016\u00036\u06db\b6\u00016\u0001"+
		"6\u00036\u06df\b6\u00036\u06e1\b6\u00016\u00016\u00016\u00016\u00016\u0003"+
		"6\u06e8\b6\u00016\u00016\u00036\u06ec\b6\u00036\u06ee\b6\u00036\u06f0"+
		"\b6\u00017\u00017\u00017\u00017\u00017\u00057\u06f7\b7\n7\f7\u06fa\t7"+
		"\u00017\u00017\u00017\u00017\u00017\u00017\u00017\u00017\u00037\u0704"+
		"\b7\u00018\u00018\u00038\u0708\b8\u00019\u00019\u00019\u00019\u00019\u0001"+
		"9\u00059\u0710\b9\n9\f9\u0713\t9\u00019\u00019\u0001:\u0001:\u0001;\u0001"+
		";\u0001;\u0003;\u071c\b;\u0001;\u0001;\u0003;\u0720\b;\u0001;\u0001;\u0001"+
		";\u0001;\u0001;\u0001;\u0005;\u0728\b;\n;\f;\u072b\t;\u0001<\u0001<\u0001"+
		"<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0003<\u0737\b<\u0001"+
		"<\u0001<\u0001<\u0001<\u0001<\u0001<\u0003<\u073f\b<\u0001<\u0001<\u0001"+
		"<\u0001<\u0001<\u0005<\u0746\b<\n<\f<\u0749\t<\u0001<\u0001<\u0001<\u0003"+
		"<\u074e\b<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0003<\u0756\b<\u0001"+
		"<\u0001<\u0001<\u0001<\u0003<\u075c\b<\u0001<\u0001<\u0003<\u0760\b<\u0001"+
		"<\u0001<\u0001<\u0003<\u0765\b<\u0001<\u0001<\u0001<\u0003<\u076a\b<\u0001"+
		"=\u0001=\u0001=\u0001=\u0003=\u0770\b=\u0001=\u0001=\u0001=\u0001=\u0001"+
		"=\u0001=\u0001=\u0001=\u0001=\u0001=\u0001=\u0001=\u0005=\u077e\b=\n="+
		"\f=\u0781\t=\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0004>\u079c\b>\u000b>\f>"+
		"\u079d\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0005>\u07a7\b"+
		">\n>\f>\u07aa\t>\u0001>\u0001>\u0001>\u0001>\u0001>\u0003>\u07b1\b>\u0001"+
		">\u0001>\u0001>\u0003>\u07b6\b>\u0001>\u0001>\u0001>\u0003>\u07bb\b>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0005>\u07c6"+
		"\b>\n>\f>\u07c9\t>\u0001>\u0001>\u0001>\u0003>\u07ce\b>\u0001>\u0001>"+
		"\u0001>\u0001>\u0001>\u0003>\u07d5\b>\u0001>\u0001>\u0001>\u0003>\u07da"+
		"\b>\u0001>\u0003>\u07dd\b>\u0001>\u0003>\u07e0\b>\u0001>\u0001>\u0001"+
		">\u0003>\u07e5\b>\u0001>\u0001>\u0001>\u0005>\u07ea\b>\n>\f>\u07ed\t>"+
		"\u0003>\u07ef\b>\u0001>\u0001>\u0001>\u0001>\u0001>\u0005>\u07f6\b>\n"+
		">\f>\u07f9\t>\u0003>\u07fb\b>\u0001>\u0001>\u0003>\u07ff\b>\u0001>\u0003"+
		">\u0802\b>\u0001>\u0003>\u0805\b>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0005>\u0812\b>\n>\f>\u0815\t>\u0003"+
		">\u0817\b>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0004>\u0828\b>\u000b>\f>"+
		"\u0829\u0001>\u0001>\u0003>\u082e\b>\u0001>\u0001>\u0001>\u0001>\u0004"+
		">\u0834\b>\u000b>\f>\u0835\u0001>\u0001>\u0003>\u083a\b>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0005"+
		">\u0851\b>\n>\f>\u0854\t>\u0003>\u0856\b>\u0001>\u0001>\u0001>\u0001>"+
		"\u0001>\u0001>\u0001>\u0003>\u085f\b>\u0001>\u0001>\u0001>\u0001>\u0003"+
		">\u0865\b>\u0001>\u0001>\u0001>\u0001>\u0003>\u086b\b>\u0001>\u0001>\u0001"+
		">\u0001>\u0003>\u0871\b>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0003>\u087a\b>\u0001>\u0003>\u087d\b>\u0001>\u0003>\u0880\b>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0003>\u0893\b>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0003>\u089c\b>\u0001>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0001>\u0005>\u08b0\b>\n>\f>\u08b3\t>\u0003"+
		">\u08b5\b>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0003"+
		">\u08bf\b>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0003>\u08c8"+
		"\b>\u0001>\u0001>\u0001>\u0001>\u0003>\u08ce\b>\u0001>\u0001>\u0001>\u0001"+
		">\u0003>\u08d4\b>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0001>\u0003>\u08df\b>\u0003>\u08e1\b>\u0001>\u0001>\u0001>\u0003>\u08e6"+
		"\b>\u0001>\u0001>\u0001>\u0001>\u0001>\u0003>\u08ed\b>\u0003>\u08ef\b"+
		">\u0001>\u0001>\u0001>\u0001>\u0003>\u08f5\b>\u0001>\u0001>\u0001>\u0001"+
		">\u0003>\u08fb\b>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0005"+
		">\u0904\b>\n>\f>\u0907\t>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0003"+
		">\u090f\b>\u0001>\u0001>\u0001>\u0003>\u0914\b>\u0001>\u0001>\u0001>\u0003"+
		">\u0919\b>\u0003>\u091b\b>\u0003>\u091d\b>\u0001>\u0001>\u0001>\u0001"+
		">\u0003>\u0923\b>\u0003>\u0925\b>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0005>\u092d\b>\n>\f>\u0930\t>\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0003>\u0938\b>\u0003>\u093a\b>\u0001>\u0001>\u0001>\u0001>\u0003>\u0940"+
		"\b>\u0003>\u0942\b>\u0001>\u0003>\u0945\b>\u0001>\u0001>\u0001>\u0001"+
		">\u0001>\u0001>\u0001>\u0001>\u0005>\u094f\b>\n>\f>\u0952\t>\u0001?\u0001"+
		"?\u0001?\u0001?\u0001?\u0001?\u0001?\u0005?\u095b\b?\n?\f?\u095e\t?\u0003"+
		"?\u0960\b?\u0001@\u0001@\u0001@\u0003@\u0965\b@\u0001A\u0001A\u0001A\u0003"+
		"A\u096a\bA\u0001B\u0001B\u0001B\u0001B\u0001C\u0001C\u0001D\u0001D\u0001"+
		"D\u0001D\u0003D\u0976\bD\u0001E\u0001E\u0003E\u097a\bE\u0001E\u0001E\u0003"+
		"E\u097e\bE\u0001E\u0003E\u0981\bE\u0003E\u0983\bE\u0001F\u0001F\u0001"+
		"F\u0001F\u0001F\u0001F\u0003F\u098b\bF\u0001G\u0003G\u098e\bG\u0001G\u0001"+
		"G\u0001G\u0001G\u0001G\u0001G\u0001G\u0001G\u0003G\u0998\bG\u0001H\u0001"+
		"H\u0001I\u0001I\u0001I\u0001I\u0003I\u09a0\bI\u0001J\u0001J\u0001J\u0001"+
		"J\u0003J\u09a6\bJ\u0003J\u09a8\bJ\u0001K\u0001K\u0001K\u0001K\u0001K\u0001"+
		"K\u0003K\u09b0\bK\u0001L\u0001L\u0001M\u0001M\u0001N\u0001N\u0001O\u0001"+
		"O\u0003O\u09ba\bO\u0001O\u0001O\u0001O\u0001O\u0003O\u09c0\bO\u0001P\u0001"+
		"P\u0001Q\u0001Q\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0005R\u09cc"+
		"\bR\nR\fR\u09cf\tR\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0003R\u09d7"+
		"\bR\u0001R\u0001R\u0001R\u0001R\u0001R\u0003R\u09de\bR\u0001R\u0001R\u0001"+
		"R\u0003R\u09e3\bR\u0001R\u0001R\u0001R\u0001R\u0001R\u0003R\u09ea\bR\u0001"+
		"R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0003R\u09f4\bR\u0001"+
		"R\u0001R\u0001R\u0003R\u09f9\bR\u0001R\u0001R\u0001R\u0001R\u0001R\u0003"+
		"R\u0a00\bR\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001"+
		"R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001"+
		"R\u0001R\u0001R\u0001R\u0005R\u0a18\bR\nR\fR\u0a1b\tR\u0001R\u0001R\u0003"+
		"R\u0a1f\bR\u0003R\u0a21\bR\u0001R\u0001R\u0001R\u0001R\u0001R\u0003R\u0a28"+
		"\bR\u0005R\u0a2a\bR\nR\fR\u0a2d\tR\u0001S\u0001S\u0001S\u0001S\u0003S"+
		"\u0a33\bS\u0001T\u0001T\u0003T\u0a37\bT\u0001U\u0001U\u0001U\u0001U\u0001"+
		"U\u0001V\u0001V\u0001V\u0001V\u0001V\u0001V\u0001W\u0001W\u0001W\u0001"+
		"W\u0003W\u0a48\bW\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001"+
		"W\u0001W\u0001W\u0001W\u0005W\u0a55\bW\nW\fW\u0a58\tW\u0001W\u0001W\u0001"+
		"W\u0001W\u0003W\u0a5e\bW\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001"+
		"W\u0003W\u0a67\bW\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0005W\u0a6f"+
		"\bW\nW\fW\u0a72\tW\u0001W\u0001W\u0003W\u0a76\bW\u0001W\u0001W\u0001W"+
		"\u0001W\u0001W\u0005W\u0a7d\bW\nW\fW\u0a80\tW\u0001W\u0001W\u0003W\u0a84"+
		"\bW\u0001X\u0001X\u0001X\u0001X\u0001X\u0001X\u0003X\u0a8c\bX\u0001Y\u0001"+
		"Y\u0001Y\u0001Y\u0005Y\u0a92\bY\nY\fY\u0a95\tY\u0003Y\u0a97\bY\u0001Y"+
		"\u0001Y\u0001Y\u0001Y\u0003Y\u0a9d\bY\u0001Y\u0003Y\u0aa0\bY\u0001Y\u0001"+
		"Y\u0001Y\u0001Y\u0001Y\u0003Y\u0aa7\bY\u0001Y\u0001Y\u0001Y\u0001Y\u0005"+
		"Y\u0aad\bY\nY\fY\u0ab0\tY\u0003Y\u0ab2\bY\u0001Y\u0001Y\u0001Y\u0001Y"+
		"\u0005Y\u0ab8\bY\nY\fY\u0abb\tY\u0003Y\u0abd\bY\u0001Z\u0001Z\u0001Z\u0001"+
		"Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001"+
		"Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001"+
		"Z\u0003Z\u0ad7\bZ\u0001[\u0001[\u0001[\u0001[\u0001[\u0001[\u0001[\u0001"+
		"[\u0001[\u0003[\u0ae2\b[\u0001\\\u0001\\\u0001\\\u0003\\\u0ae7\b\\\u0001"+
		"\\\u0001\\\u0001\\\u0001\\\u0001\\\u0005\\\u0aee\b\\\n\\\f\\\u0af1\t\\"+
		"\u0001]\u0001]\u0001]\u0001]\u0001]\u0001]\u0001]\u0001]\u0005]\u0afb"+
		"\b]\n]\f]\u0afe\t]\u0001]\u0001]\u0001]\u0001]\u0001]\u0001]\u0001]\u0001"+
		"]\u0001]\u0001]\u0001]\u0001]\u0003]\u0b0c\b]\u0001^\u0001^\u0003^\u0b10"+
		"\b^\u0001^\u0001^\u0003^\u0b14\b^\u0001^\u0001^\u0003^\u0b18\b^\u0001"+
		"^\u0001^\u0001^\u0001^\u0003^\u0b1e\b^\u0001^\u0001^\u0003^\u0b22\b^\u0001"+
		"^\u0001^\u0003^\u0b26\b^\u0001^\u0001^\u0003^\u0b2a\b^\u0003^\u0b2c\b"+
		"^\u0001_\u0001_\u0001_\u0001_\u0001`\u0001`\u0001`\u0001`\u0003`\u0b36"+
		"\b`\u0001a\u0001a\u0001a\u0001a\u0001a\u0003a\u0b3d\ba\u0001b\u0001b\u0001"+
		"b\u0001b\u0001b\u0001b\u0001b\u0003b\u0b46\bb\u0001c\u0001c\u0001c\u0001"+
		"c\u0001c\u0003c\u0b4d\bc\u0001d\u0001d\u0001d\u0001d\u0001d\u0003d\u0b54"+
		"\bd\u0001e\u0001e\u0001e\u0005e\u0b59\be\ne\fe\u0b5c\te\u0001f\u0001f"+
		"\u0001g\u0001g\u0001g\u0005g\u0b63\bg\ng\fg\u0b66\tg\u0001h\u0001h\u0001"+
		"h\u0001h\u0001h\u0001h\u0001i\u0001i\u0001j\u0001j\u0001j\u0003j\u0b73"+
		"\bj\u0001k\u0001k\u0001k\u0001k\u0001k\u0003k\u0b7a\bk\u0001l\u0001l\u0001"+
		"l\u0005l\u0b7f\bl\nl\fl\u0b82\tl\u0001m\u0001m\u0001m\u0001m\u0001m\u0003"+
		"m\u0b89\bm\u0001n\u0003n\u0b8c\bn\u0001n\u0001n\u0003n\u0b90\bn\u0001"+
		"n\u0001n\u0003n\u0b94\bn\u0001n\u0003n\u0b97\bn\u0001o\u0001o\u0001o\u0000"+
		"\u0007*Bvz|\u00a4\u00b8p\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012"+
		"\u0014\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\"+
		"^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090"+
		"\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8"+
		"\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be\u00c0"+
		"\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8"+
		"\u00da\u00dc\u00de\u0000\"\u0002\u0000%%\u00d3\u00d3\u0002\u0000EEzz\u0002"+
		"\u0000\u00df\u00df\u00f0\u00f0\u0002\u0000ccqq\u0002\u0000VVrr\u0001\u0000"+
		"\u00db\u00dc\u0002\u0000__\u009f\u009f\u0002\u0000\u012e\u012e\u0132\u0132"+
		"\u0002\u0000UU\u0105\u0105\u0002\u0000\u001d\u001dHH\u0002\u0000__\u0088"+
		"\u0088\u0002\u0000\u0016\u0016KK\u0002\u0000  \u00ef\u00ef\u0003\u0000"+
		"\"\"\u008a\u008a\u00fa\u00fa\u0002\u0000ss\u00e3\u00e3\u0001\u0000\u0128"+
		"\u0129\u0001\u0000\u012a\u012c\u0002\u0000\u0085\u0085\u00ae\u00ae\u0001"+
		"\u0000\u010e\u0110\u0004\u0000SS[[\u00fd\u00fd\u0107\u0107\u0002\u0000"+
		"//\u0104\u0104\u0002\u0000^^\u00dd\u00dd\u0001\u0000\u0122\u0127\u0003"+
		"\u0000\u0016\u0016\u001a\u001a\u00ea\u00ea\u0002\u0000[[\u00fd\u00fd\u0005"+
		"\u0000AAnn\u009c\u009d\u00e1\u00e1\u0120\u0120\u0001\u0000\u00a0\u00a3"+
		"\u0002\u0000``\u00c3\u00c3\u0003\u0000ii\u007f\u007f\u00f3\u00f3\u0004"+
		"\u0000LL{{\u0093\u0093\u0111\u0111\u0002\u0000\u00b1\u00b1\u011f\u011f"+
		"\u0005\u000033FFvv\u00e4\u00e4\u010a\u010a\u0002\u0000\u00f8\u00f8\u0115"+
		"\u01156\u0000\u0012\u0016\u0018\u0018\u001a\u001b\u001d \"\"$%(/1266?"+
		"ACEGHJJLMPQSSVVYY\\`bbeillnprsuuxxz{}}\u007f\u007f\u0085\u008a\u008c\u008c"+
		"\u008e\u008e\u0090\u0090\u0093\u009d\u009f\u00a5\u00a9\u00ae\u00b0\u00b2"+
		"\u00b5\u00b5\u00b7\u00c4\u00c6\u00cb\u00cd\u00d5\u00d7\u00d9\u00db\u00e3"+
		"\u00e5\u00ef\u00f1\u00f4\u00f6\u00fb\u00fe\u0100\u0102\u0104\u0106\u0108"+
		"\u010a\u010c\u010e\u0112\u0114\u0116\u0119\u0119\u011b\u0121\u0d6e\u0000"+
		"\u00e0\u0001\u0000\u0000\u0000\u0002\u00e3\u0001\u0000\u0000\u0000\u0004"+
		"\u00e6\u0001\u0000\u0000\u0000\u0006\u00e9\u0001\u0000\u0000\u0000\b\u00ec"+
		"\u0001\u0000\u0000\u0000\n\u0404\u0001\u0000\u0000\u0000\f\u0407\u0001"+
		"\u0000\u0000\u0000\u000e\u040b\u0001\u0000\u0000\u0000\u0010\u0419\u0001"+
		"\u0000\u0000\u0000\u0012\u041b\u0001\u0000\u0000\u0000\u0014\u0429\u0001"+
		"\u0000\u0000\u0000\u0016\u042f\u0001\u0000\u0000\u0000\u0018\u0433\u0001"+
		"\u0000\u0000\u0000\u001a\u043b\u0001\u0000\u0000\u0000\u001c\u0441\u0001"+
		"\u0000\u0000\u0000\u001e\u0443\u0001\u0000\u0000\u0000 \u0447\u0001\u0000"+
		"\u0000\u0000\"\u044f\u0001\u0000\u0000\u0000$\u0453\u0001\u0000\u0000"+
		"\u0000&\u0478\u0001\u0000\u0000\u0000(\u047a\u0001\u0000\u0000\u0000*"+
		"\u047c\u0001\u0000\u0000\u0000,\u04a0\u0001\u0000\u0000\u0000.\u04a2\u0001"+
		"\u0000\u0000\u00000\u04aa\u0001\u0000\u0000\u00002\u04da\u0001\u0000\u0000"+
		"\u00004\u050c\u0001\u0000\u0000\u00006\u051b\u0001\u0000\u0000\u00008"+
		"\u051d\u0001\u0000\u0000\u0000:\u0524\u0001\u0000\u0000\u0000<\u0541\u0001"+
		"\u0000\u0000\u0000>\u054a\u0001\u0000\u0000\u0000@\u055b\u0001\u0000\u0000"+
		"\u0000B\u055d\u0001\u0000\u0000\u0000D\u0584\u0001\u0000\u0000\u0000F"+
		"\u0594\u0001\u0000\u0000\u0000H\u0596\u0001\u0000\u0000\u0000J\u059f\u0001"+
		"\u0000\u0000\u0000L\u05a1\u0001\u0000\u0000\u0000N\u05a9\u0001\u0000\u0000"+
		"\u0000P\u05af\u0001\u0000\u0000\u0000R\u05b1\u0001\u0000\u0000\u0000T"+
		"\u0606\u0001\u0000\u0000\u0000V\u0615\u0001\u0000\u0000\u0000X\u0620\u0001"+
		"\u0000\u0000\u0000Z\u0635\u0001\u0000\u0000\u0000\\\u0637\u0001\u0000"+
		"\u0000\u0000^\u0644\u0001\u0000\u0000\u0000`\u0648\u0001\u0000\u0000\u0000"+
		"b\u0652\u0001\u0000\u0000\u0000d\u0682\u0001\u0000\u0000\u0000f\u0684"+
		"\u0001\u0000\u0000\u0000h\u06a0\u0001\u0000\u0000\u0000j\u06a7\u0001\u0000"+
		"\u0000\u0000l\u06ef\u0001\u0000\u0000\u0000n\u0703\u0001\u0000\u0000\u0000"+
		"p\u0705\u0001\u0000\u0000\u0000r\u0709\u0001\u0000\u0000\u0000t\u0716"+
		"\u0001\u0000\u0000\u0000v\u071f\u0001\u0000\u0000\u0000x\u0769\u0001\u0000"+
		"\u0000\u0000z\u076f\u0001\u0000\u0000\u0000|\u0944\u0001\u0000\u0000\u0000"+
		"~\u0953\u0001\u0000\u0000\u0000\u0080\u0961\u0001\u0000\u0000\u0000\u0082"+
		"\u0966\u0001\u0000\u0000\u0000\u0084\u096b\u0001\u0000\u0000\u0000\u0086"+
		"\u096f\u0001\u0000\u0000\u0000\u0088\u0975\u0001\u0000\u0000\u0000\u008a"+
		"\u0982\u0001\u0000\u0000\u0000\u008c\u098a\u0001\u0000\u0000\u0000\u008e"+
		"\u0997\u0001\u0000\u0000\u0000\u0090\u0999\u0001\u0000\u0000\u0000\u0092"+
		"\u099f\u0001\u0000\u0000\u0000\u0094\u09a7\u0001\u0000\u0000\u0000\u0096"+
		"\u09af\u0001\u0000\u0000\u0000\u0098\u09b1\u0001\u0000\u0000\u0000\u009a"+
		"\u09b3\u0001\u0000\u0000\u0000\u009c\u09b5\u0001\u0000\u0000\u0000\u009e"+
		"\u09b7\u0001\u0000\u0000\u0000\u00a0\u09c1\u0001\u0000\u0000\u0000\u00a2"+
		"\u09c3\u0001\u0000\u0000\u0000\u00a4\u0a20\u0001\u0000\u0000\u0000\u00a6"+
		"\u0a32\u0001\u0000\u0000\u0000\u00a8\u0a36\u0001\u0000\u0000\u0000\u00aa"+
		"\u0a38\u0001\u0000\u0000\u0000\u00ac\u0a3d\u0001\u0000\u0000\u0000\u00ae"+
		"\u0a83\u0001\u0000\u0000\u0000\u00b0\u0a85\u0001\u0000\u0000\u0000\u00b2"+
		"\u0a96\u0001\u0000\u0000\u0000\u00b4\u0ad6\u0001\u0000\u0000\u0000\u00b6"+
		"\u0ae1\u0001\u0000\u0000\u0000\u00b8\u0ae3\u0001\u0000\u0000\u0000\u00ba"+
		"\u0b0b\u0001\u0000\u0000\u0000\u00bc\u0b2b\u0001\u0000\u0000\u0000\u00be"+
		"\u0b2d\u0001\u0000\u0000\u0000\u00c0\u0b35\u0001\u0000\u0000\u0000\u00c2"+
		"\u0b3c\u0001\u0000\u0000\u0000\u00c4\u0b45\u0001\u0000\u0000\u0000\u00c6"+
		"\u0b4c\u0001\u0000\u0000\u0000\u00c8\u0b53\u0001\u0000\u0000\u0000\u00ca"+
		"\u0b55\u0001\u0000\u0000\u0000\u00cc\u0b5d\u0001\u0000\u0000\u0000\u00ce"+
		"\u0b5f\u0001\u0000\u0000\u0000\u00d0\u0b67\u0001\u0000\u0000\u0000\u00d2"+
		"\u0b6d\u0001\u0000\u0000\u0000\u00d4\u0b72\u0001\u0000\u0000\u0000\u00d6"+
		"\u0b79\u0001\u0000\u0000\u0000\u00d8\u0b7b\u0001\u0000\u0000\u0000\u00da"+
		"\u0b88\u0001\u0000\u0000\u0000\u00dc\u0b96\u0001\u0000\u0000\u0000\u00de"+
		"\u0b98\u0001\u0000\u0000\u0000\u00e0\u00e1\u0003\n\u0005\u0000\u00e1\u00e2"+
		"\u0005\u0000\u0000\u0001\u00e2\u0001\u0001\u0000\u0000\u0000\u00e3\u00e4"+
		"\u0003t:\u0000\u00e4\u00e5\u0005\u0000\u0000\u0001\u00e5\u0003\u0001\u0000"+
		"\u0000\u0000\u00e6\u00e7\u0003\u00cae\u0000\u00e7\u00e8\u0005\u0000\u0000"+
		"\u0001\u00e8\u0005\u0001\u0000\u0000\u0000\u00e9\u00ea\u0003\u00a4R\u0000"+
		"\u00ea\u00eb\u0005\u0000\u0000\u0001\u00eb\u0007\u0001\u0000\u0000\u0000"+
		"\u00ec\u00ed\u0003\u00b8\\\u0000\u00ed\u00ee\u0005\u0000\u0000\u0001\u00ee"+
		"\t\u0001\u0000\u0000\u0000\u00ef\u0405\u0003\f\u0006\u0000\u00f0\u00f1"+
		"\u0005\u010b\u0000\u0000\u00f1\u0405\u0003\u00dam\u0000\u00f2\u00f3\u0005"+
		"\u010b\u0000\u0000\u00f3\u00f4\u0003\u00dam\u0000\u00f4\u00f5\u0005\u0001"+
		"\u0000\u0000\u00f5\u00f6\u0003\u00dam\u0000\u00f6\u0405\u0001\u0000\u0000"+
		"\u0000\u00f7\u00f8\u00053\u0000\u0000\u00f8\u00fc\u0005(\u0000\u0000\u00f9"+
		"\u00fa\u0005o\u0000\u0000\u00fa\u00fb\u0005\u00a7\u0000\u0000\u00fb\u00fd"+
		"\u0005X\u0000\u0000\u00fc\u00f9\u0001\u0000\u0000\u0000\u00fc\u00fd\u0001"+
		"\u0000\u0000\u0000\u00fd\u00fe\u0001\u0000\u0000\u0000\u00fe\u0101\u0003"+
		"\u00ceg\u0000\u00ff\u0100\u0005,\u0000\u0000\u0100\u0102\u0003\u0094J"+
		"\u0000\u0101\u00ff\u0001\u0000\u0000\u0000\u0101\u0102\u0001\u0000\u0000"+
		"\u0000\u0102\u0105\u0001\u0000\u0000\u0000\u0103\u0104\u0005\u011a\u0000"+
		"\u0000\u0104\u0106\u0003\u001e\u000f\u0000\u0105\u0103\u0001\u0000\u0000"+
		"\u0000\u0105\u0106\u0001\u0000\u0000\u0000\u0106\u0405\u0001\u0000\u0000"+
		"\u0000\u0107\u0108\u00053\u0000\u0000\u0108\u010c\u0005\u00df\u0000\u0000"+
		"\u0109\u010a\u0005o\u0000\u0000\u010a\u010b\u0005\u00a7\u0000\u0000\u010b"+
		"\u010d\u0005X\u0000\u0000\u010c\u0109\u0001\u0000\u0000\u0000\u010c\u010d"+
		"\u0001\u0000\u0000\u0000\u010d\u010e\u0001\u0000\u0000\u0000\u010e\u0111"+
		"\u0003\u00ceg\u0000\u010f\u0110\u0005\u001f\u0000\u0000\u0110\u0112\u0003"+
		"\u00d6k\u0000\u0111\u010f\u0001\u0000\u0000\u0000\u0111\u0112\u0001\u0000"+
		"\u0000\u0000\u0112\u0115\u0001\u0000\u0000\u0000\u0113\u0114\u0005\u011a"+
		"\u0000\u0000\u0114\u0116\u0003\u0016\u000b\u0000\u0115\u0113\u0001\u0000"+
		"\u0000\u0000\u0115\u0116\u0001\u0000\u0000\u0000\u0116\u0405\u0001\u0000"+
		"\u0000\u0000\u0117\u0118\u0005N\u0000\u0000\u0118\u011b\u0005\u00df\u0000"+
		"\u0000\u0119\u011a\u0005o\u0000\u0000\u011a\u011c\u0005X\u0000\u0000\u011b"+
		"\u0119\u0001\u0000\u0000\u0000\u011b\u011c\u0001\u0000\u0000\u0000\u011c"+
		"\u011d\u0001\u0000\u0000\u0000\u011d\u011f\u0003\u00ceg\u0000\u011e\u0120"+
		"\u0007\u0000\u0000\u0000\u011f\u011e\u0001\u0000\u0000\u0000\u011f\u0120"+
		"\u0001\u0000\u0000\u0000\u0120\u0405\u0001\u0000\u0000\u0000\u0121\u0122"+
		"\u0005\u0017\u0000\u0000\u0122\u0123\u0005\u00df\u0000\u0000\u0123\u0124"+
		"\u0003\u00ceg\u0000\u0124\u0125\u0005\u00ce\u0000\u0000\u0125\u0126\u0005"+
		"\u00f9\u0000\u0000\u0126\u0127\u0003\u00dam\u0000\u0127\u0405\u0001\u0000"+
		"\u0000\u0000\u0128\u0129\u0005\u0017\u0000\u0000\u0129\u012a\u0005\u00df"+
		"\u0000\u0000\u012a\u012b\u0003\u00ceg\u0000\u012b\u012c\u0005\u00e7\u0000"+
		"\u0000\u012c\u012d\u0005\u001f\u0000\u0000\u012d\u012e\u0003\u00d6k\u0000"+
		"\u012e\u0405\u0001\u0000\u0000\u0000\u012f\u0130\u00053\u0000\u0000\u0130"+
		"\u0134\u0005\u00f0\u0000\u0000\u0131\u0132\u0005o\u0000\u0000\u0132\u0133"+
		"\u0005\u00a7\u0000\u0000\u0133\u0135\u0005X\u0000\u0000\u0134\u0131\u0001"+
		"\u0000\u0000\u0000\u0134\u0135\u0001\u0000\u0000\u0000\u0135\u0136\u0001"+
		"\u0000\u0000\u0000\u0136\u0138\u0003\u00ceg\u0000\u0137\u0139\u0003b1"+
		"\u0000\u0138\u0137\u0001\u0000\u0000\u0000\u0138\u0139\u0001\u0000\u0000"+
		"\u0000\u0139\u013c\u0001\u0000\u0000\u0000\u013a\u013b\u0005,\u0000\u0000"+
		"\u013b\u013d\u0003\u0094J\u0000\u013c\u013a\u0001\u0000\u0000\u0000\u013c"+
		"\u013d\u0001\u0000\u0000\u0000\u013d\u0140\u0001\u0000\u0000\u0000\u013e"+
		"\u013f\u0005\u011a\u0000\u0000\u013f\u0141\u0003\u0016\u000b\u0000\u0140"+
		"\u013e\u0001\u0000\u0000\u0000\u0140\u0141\u0001\u0000\u0000\u0000\u0141"+
		"\u0142\u0001\u0000\u0000\u0000\u0142\u0148\u0005\u001c\u0000\u0000\u0143"+
		"\u0149\u0003\f\u0006\u0000\u0144\u0145\u0005\u0002\u0000\u0000\u0145\u0146"+
		"\u0003\f\u0006\u0000\u0146\u0147\u0005\u0003\u0000\u0000\u0147\u0149\u0001"+
		"\u0000\u0000\u0000\u0148\u0143\u0001\u0000\u0000\u0000\u0148\u0144\u0001"+
		"\u0000\u0000\u0000\u0149\u014f\u0001\u0000\u0000\u0000\u014a\u014c\u0005"+
		"\u011a\u0000\u0000\u014b\u014d\u0005\u00a4\u0000\u0000\u014c\u014b\u0001"+
		"\u0000\u0000\u0000\u014c\u014d\u0001\u0000\u0000\u0000\u014d\u014e\u0001"+
		"\u0000\u0000\u0000\u014e\u0150\u0005?\u0000\u0000\u014f\u014a\u0001\u0000"+
		"\u0000\u0000\u014f\u0150\u0001\u0000\u0000\u0000\u0150\u0405\u0001\u0000"+
		"\u0000\u0000\u0151\u0152\u00053\u0000\u0000\u0152\u0156\u0005\u00f0\u0000"+
		"\u0000\u0153\u0154\u0005o\u0000\u0000\u0154\u0155\u0005\u00a7\u0000\u0000"+
		"\u0155\u0157\u0005X\u0000\u0000\u0156\u0153\u0001\u0000\u0000\u0000\u0156"+
		"\u0157\u0001\u0000\u0000\u0000\u0157\u0158\u0001\u0000\u0000\u0000\u0158"+
		"\u0159\u0003\u00ceg\u0000\u0159\u015a\u0005\u0002\u0000\u0000\u015a\u015f"+
		"\u0003\u0010\b\u0000\u015b\u015c\u0005\u0004\u0000\u0000\u015c\u015e\u0003"+
		"\u0010\b\u0000\u015d\u015b\u0001\u0000\u0000\u0000\u015e\u0161\u0001\u0000"+
		"\u0000\u0000\u015f\u015d\u0001\u0000\u0000\u0000\u015f\u0160\u0001\u0000"+
		"\u0000\u0000\u0160\u0162\u0001\u0000\u0000\u0000\u0161\u015f\u0001\u0000"+
		"\u0000\u0000\u0162\u0165\u0005\u0003\u0000\u0000\u0163\u0164\u0005,\u0000"+
		"\u0000\u0164\u0166\u0003\u0094J\u0000\u0165\u0163\u0001\u0000\u0000\u0000"+
		"\u0165\u0166\u0001\u0000\u0000\u0000\u0166\u0169\u0001\u0000\u0000\u0000"+
		"\u0167\u0168\u0005\u011a\u0000\u0000\u0168\u016a\u0003\u0016\u000b\u0000"+
		"\u0169\u0167\u0001\u0000\u0000\u0000\u0169\u016a\u0001\u0000\u0000\u0000"+
		"\u016a\u0405\u0001\u0000\u0000\u0000\u016b\u016c\u0005N\u0000\u0000\u016c"+
		"\u016f\u0005\u00f0\u0000\u0000\u016d\u016e\u0005o\u0000\u0000\u016e\u0170"+
		"\u0005X\u0000\u0000\u016f\u016d\u0001\u0000\u0000\u0000\u016f\u0170\u0001"+
		"\u0000\u0000\u0000\u0170\u0171\u0001\u0000\u0000\u0000\u0171\u0405\u0003"+
		"\u00ceg\u0000\u0172\u0173\u0005v\u0000\u0000\u0173\u0174\u0005y\u0000"+
		"\u0000\u0174\u0176\u0003\u00ceg\u0000\u0175\u0177\u0003b1\u0000\u0176"+
		"\u0175\u0001\u0000\u0000\u0000\u0176\u0177\u0001\u0000\u0000\u0000\u0177"+
		"\u0178\u0001\u0000\u0000\u0000\u0178\u0179\u0003\f\u0006\u0000\u0179\u0405"+
		"\u0001\u0000\u0000\u0000\u017a\u017b\u0005F\u0000\u0000\u017b\u017c\u0005"+
		"c\u0000\u0000\u017c\u017f\u0003\u00ceg\u0000\u017d\u017e\u0005\u0118\u0000"+
		"\u0000\u017e\u0180\u0003v;\u0000\u017f\u017d\u0001\u0000\u0000\u0000\u017f"+
		"\u0180\u0001\u0000\u0000\u0000\u0180\u0405\u0001\u0000\u0000\u0000\u0181"+
		"\u0182\u0005\u00fe\u0000\u0000\u0182\u0183\u0005\u00f0\u0000\u0000\u0183"+
		"\u0405\u0003\u00ceg\u0000\u0184\u0185\u0005,\u0000\u0000\u0185\u0186\u0005"+
		"\u00af\u0000\u0000\u0186\u0187\u0005\u00f0\u0000\u0000\u0187\u0188\u0003"+
		"\u00ceg\u0000\u0188\u018b\u0005|\u0000\u0000\u0189\u018c\u0003\u0094J"+
		"\u0000\u018a\u018c\u0005\u00a8\u0000\u0000\u018b\u0189\u0001\u0000\u0000"+
		"\u0000\u018b\u018a\u0001\u0000\u0000\u0000\u018c\u0405\u0001\u0000\u0000"+
		"\u0000\u018d\u018e\u0005,\u0000\u0000\u018e\u018f\u0005\u00af\u0000\u0000"+
		"\u018f\u0190\u0005\u0116\u0000\u0000\u0190\u0191\u0003\u00ceg\u0000\u0191"+
		"\u0194\u0005|\u0000\u0000\u0192\u0195\u0003\u0094J\u0000\u0193\u0195\u0005"+
		"\u00a8\u0000\u0000\u0194\u0192\u0001\u0000\u0000\u0000\u0194\u0193\u0001"+
		"\u0000\u0000\u0000\u0195\u0405\u0001\u0000\u0000\u0000\u0196\u0197\u0005"+
		",\u0000\u0000\u0197\u0198\u0005\u00af\u0000\u0000\u0198\u0199\u0005*\u0000"+
		"\u0000\u0199\u019a\u0003\u00ceg\u0000\u019a\u019d\u0005|\u0000\u0000\u019b"+
		"\u019e\u0003\u0094J\u0000\u019c\u019e\u0005\u00a8\u0000\u0000\u019d\u019b"+
		"\u0001\u0000\u0000\u0000\u019d\u019c\u0001\u0000\u0000\u0000\u019e\u0405"+
		"\u0001\u0000\u0000\u0000\u019f\u01a0\u0005\u0017\u0000\u0000\u01a0\u01a3"+
		"\u0005\u00f0\u0000\u0000\u01a1\u01a2\u0005o\u0000\u0000\u01a2\u01a4\u0005"+
		"X\u0000\u0000\u01a3\u01a1\u0001\u0000\u0000\u0000\u01a3\u01a4\u0001\u0000"+
		"\u0000\u0000\u01a4\u01a5\u0001\u0000\u0000\u0000\u01a5\u01a6\u0003\u00ce"+
		"g\u0000\u01a6\u01a7\u0005\u00ce\u0000\u0000\u01a7\u01a8\u0005\u00f9\u0000"+
		"\u0000\u01a8\u01a9\u0003\u00ceg\u0000\u01a9\u0405\u0001\u0000\u0000\u0000"+
		"\u01aa\u01ab\u0005\u0017\u0000\u0000\u01ab\u01ae\u0005\u00f0\u0000\u0000"+
		"\u01ac\u01ad\u0005o\u0000\u0000\u01ad\u01af\u0005X\u0000\u0000\u01ae\u01ac"+
		"\u0001\u0000\u0000\u0000\u01ae\u01af\u0001\u0000\u0000\u0000\u01af\u01b0"+
		"\u0001\u0000\u0000\u0000\u01b0\u01b1\u0003\u00ceg\u0000\u01b1\u01b2\u0005"+
		"\u0013\u0000\u0000\u01b2\u01b6\u0005*\u0000\u0000\u01b3\u01b4\u0005o\u0000"+
		"\u0000\u01b4\u01b5\u0005\u00a7\u0000\u0000\u01b5\u01b7\u0005X\u0000\u0000"+
		"\u01b6\u01b3\u0001\u0000\u0000\u0000\u01b6\u01b7\u0001\u0000\u0000\u0000"+
		"\u01b7\u01b8\u0001\u0000\u0000\u0000\u01b8\u01b9\u0003\u0012\t\u0000\u01b9"+
		"\u0405\u0001\u0000\u0000\u0000\u01ba\u01bb\u0005\u0017\u0000\u0000\u01bb"+
		"\u01be\u0005\u00f0\u0000\u0000\u01bc\u01bd\u0005o\u0000\u0000\u01bd\u01bf"+
		"\u0005X\u0000\u0000\u01be\u01bc\u0001\u0000\u0000\u0000\u01be\u01bf\u0001"+
		"\u0000\u0000\u0000\u01bf\u01c0\u0001\u0000\u0000\u0000\u01c0\u01c1\u0003"+
		"\u00ceg\u0000\u01c1\u01c2\u0005\u00ce\u0000\u0000\u01c2\u01c5\u0005*\u0000"+
		"\u0000\u01c3\u01c4\u0005o\u0000\u0000\u01c4\u01c6\u0005X\u0000\u0000\u01c5"+
		"\u01c3\u0001\u0000\u0000\u0000\u01c5\u01c6\u0001\u0000\u0000\u0000\u01c6"+
		"\u01c7\u0001\u0000\u0000\u0000\u01c7\u01c8\u0003\u00dam\u0000\u01c8\u01c9"+
		"\u0005\u00f9\u0000\u0000\u01c9\u01ca\u0003\u00dam\u0000\u01ca\u0405\u0001"+
		"\u0000\u0000\u0000\u01cb\u01cc\u0005\u0017\u0000\u0000\u01cc\u01cf\u0005"+
		"\u00f0\u0000\u0000\u01cd\u01ce\u0005o\u0000\u0000\u01ce\u01d0\u0005X\u0000"+
		"\u0000\u01cf\u01cd\u0001\u0000\u0000\u0000\u01cf\u01d0\u0001\u0000\u0000"+
		"\u0000\u01d0\u01d1\u0001\u0000\u0000\u0000\u01d1\u01d2\u0003\u00ceg\u0000"+
		"\u01d2\u01d3\u0005N\u0000\u0000\u01d3\u01d6\u0005*\u0000\u0000\u01d4\u01d5"+
		"\u0005o\u0000\u0000\u01d5\u01d7\u0005X\u0000\u0000\u01d6\u01d4\u0001\u0000"+
		"\u0000\u0000\u01d6\u01d7\u0001\u0000\u0000\u0000\u01d7\u01d8\u0001\u0000"+
		"\u0000\u0000\u01d8\u01d9\u0003\u00ceg\u0000\u01d9\u0405\u0001\u0000\u0000"+
		"\u0000\u01da\u01db\u0005\u0017\u0000\u0000\u01db\u01dc\u0005\u00f0\u0000"+
		"\u0000\u01dc\u01dd\u0003\u00ceg\u0000\u01dd\u01de\u0005\u00e7\u0000\u0000"+
		"\u01de\u01df\u0005\u001f\u0000\u0000\u01df\u01e0\u0003\u00d6k\u0000\u01e0"+
		"\u0405\u0001\u0000\u0000\u0000\u01e1\u01e2\u0005\u0017\u0000\u0000\u01e2"+
		"\u01e3\u0005\u00f0\u0000\u0000\u01e3\u01e4\u0003\u00ceg\u0000\u01e4\u01e5"+
		"\u0005\u00e7\u0000\u0000\u01e5\u01e6\u0005\u00c7\u0000\u0000\u01e6\u01e7"+
		"\u0003\u0018\f\u0000\u01e7\u0405\u0001\u0000\u0000\u0000\u01e8\u01e9\u0005"+
		"\u0017\u0000\u0000\u01e9\u01ea\u0005\u00f0\u0000\u0000\u01ea\u01eb\u0003"+
		"\u00ceg\u0000\u01eb\u01ec\u0005W\u0000\u0000\u01ec\u01f9\u0003\u00dam"+
		"\u0000\u01ed\u01f6\u0005\u0002\u0000\u0000\u01ee\u01f3\u0003\u00c6c\u0000"+
		"\u01ef\u01f0\u0005\u0004\u0000\u0000\u01f0\u01f2\u0003\u00c6c\u0000\u01f1"+
		"\u01ef\u0001\u0000\u0000\u0000\u01f2\u01f5\u0001\u0000\u0000\u0000\u01f3"+
		"\u01f1\u0001\u0000\u0000\u0000\u01f3\u01f4\u0001\u0000\u0000\u0000\u01f4"+
		"\u01f7\u0001\u0000\u0000\u0000\u01f5\u01f3\u0001\u0000\u0000\u0000\u01f6"+
		"\u01ee\u0001\u0000\u0000\u0000\u01f6\u01f7\u0001\u0000\u0000\u0000\u01f7"+
		"\u01f8\u0001\u0000\u0000\u0000\u01f8\u01fa\u0005\u0003\u0000\u0000\u01f9"+
		"\u01ed\u0001\u0000\u0000\u0000\u01f9\u01fa\u0001\u0000\u0000\u0000\u01fa"+
		"\u01fd\u0001\u0000\u0000\u0000\u01fb\u01fc\u0005\u0118\u0000\u0000\u01fc"+
		"\u01fe\u0003v;\u0000\u01fd\u01fb\u0001\u0000\u0000\u0000\u01fd\u01fe\u0001"+
		"\u0000\u0000\u0000\u01fe\u0405\u0001\u0000\u0000\u0000\u01ff\u0200\u0005"+
		"\u0018\u0000\u0000\u0200\u0203\u0003\u00ceg\u0000\u0201\u0202\u0005\u011a"+
		"\u0000\u0000\u0202\u0204\u0003\u0016\u000b\u0000\u0203\u0201\u0001\u0000"+
		"\u0000\u0000\u0203\u0204\u0001\u0000\u0000\u0000\u0204\u0405\u0001\u0000"+
		"\u0000\u0000\u0205\u0208\u00053\u0000\u0000\u0206\u0207\u0005\u00b3\u0000"+
		"\u0000\u0207\u0209\u0005\u00d0\u0000\u0000\u0208\u0206\u0001\u0000\u0000"+
		"\u0000\u0208\u0209\u0001\u0000\u0000\u0000\u0209\u020a\u0001\u0000\u0000"+
		"\u0000\u020a\u020b\u0005\u0099\u0000\u0000\u020b\u020f\u0005\u0116\u0000"+
		"\u0000\u020c\u020d\u0005o\u0000\u0000\u020d\u020e\u0005\u00a7\u0000\u0000"+
		"\u020e\u0210\u0005X\u0000\u0000\u020f\u020c\u0001\u0000\u0000\u0000\u020f"+
		"\u0210\u0001\u0000\u0000\u0000\u0210\u0211\u0001\u0000\u0000\u0000\u0211"+
		"\u0214\u0003\u00ceg\u0000\u0212\u0213\u0005,\u0000\u0000\u0213\u0215\u0003"+
		"\u0094J\u0000\u0214\u0212\u0001\u0000\u0000\u0000\u0214\u0215\u0001\u0000"+
		"\u0000\u0000\u0215\u0218\u0001\u0000\u0000\u0000\u0216\u0217\u0005\u011a"+
		"\u0000\u0000\u0217\u0219\u0003\u0016\u000b\u0000\u0218\u0216\u0001\u0000"+
		"\u0000\u0000\u0218\u0219\u0001\u0000\u0000\u0000\u0219\u021a\u0001\u0000"+
		"\u0000\u0000\u021a\u021b\u0005\u001c\u0000\u0000\u021b\u021c\u0003\f\u0006"+
		"\u0000\u021c\u0405\u0001\u0000\u0000\u0000\u021d\u0220\u00053\u0000\u0000"+
		"\u021e\u021f\u0005\u00b3\u0000\u0000\u021f\u0221\u0005\u00d0\u0000\u0000"+
		"\u0220\u021e\u0001\u0000\u0000\u0000\u0220\u0221\u0001\u0000\u0000\u0000"+
		"\u0221\u0222\u0001\u0000\u0000\u0000\u0222\u0223\u0005\u0116\u0000\u0000"+
		"\u0223\u0226\u0003\u00ceg\u0000\u0224\u0225\u0005,\u0000\u0000\u0225\u0227"+
		"\u0003\u0094J\u0000\u0226\u0224\u0001\u0000\u0000\u0000\u0226\u0227\u0001"+
		"\u0000\u0000\u0000\u0227\u022a\u0001\u0000\u0000\u0000\u0228\u0229\u0005"+
		"\u00e2\u0000\u0000\u0229\u022b\u0007\u0001\u0000\u0000\u022a\u0228\u0001"+
		"\u0000\u0000\u0000\u022a\u022b\u0001\u0000\u0000\u0000\u022b\u022c\u0001"+
		"\u0000\u0000\u0000\u022c\u022d\u0005\u001c\u0000\u0000\u022d\u022e\u0003"+
		"\f\u0006\u0000\u022e\u0405\u0001\u0000\u0000\u0000\u022f\u0230\u0005\u00cd"+
		"\u0000\u0000\u0230\u0231\u0005\u0099\u0000\u0000\u0231\u0232\u0005\u0116"+
		"\u0000\u0000\u0232\u0405\u0003\u00ceg\u0000\u0233\u0234\u0005N\u0000\u0000"+
		"\u0234\u0235\u0005\u0099\u0000\u0000\u0235\u0238\u0005\u0116\u0000\u0000"+
		"\u0236\u0237\u0005o\u0000\u0000\u0237\u0239\u0005X\u0000\u0000\u0238\u0236"+
		"\u0001\u0000\u0000\u0000\u0238\u0239\u0001\u0000\u0000\u0000\u0239\u023a"+
		"\u0001\u0000\u0000\u0000\u023a\u0405\u0003\u00ceg\u0000\u023b\u023c\u0005"+
		"\u0017\u0000\u0000\u023c\u023d\u0005\u0099\u0000\u0000\u023d\u0240\u0005"+
		"\u0116\u0000\u0000\u023e\u023f\u0005o\u0000\u0000\u023f\u0241\u0005X\u0000"+
		"\u0000\u0240\u023e\u0001\u0000\u0000\u0000\u0240\u0241\u0001\u0000\u0000"+
		"\u0000\u0241\u0242\u0001\u0000\u0000\u0000\u0242\u0243\u0003\u00ceg\u0000"+
		"\u0243\u0244\u0005\u00ce\u0000\u0000\u0244\u0245\u0005\u00f9\u0000\u0000"+
		"\u0245\u0246\u0003\u00ceg\u0000\u0246\u0405\u0001\u0000\u0000\u0000\u0247"+
		"\u0248\u0005\u0017\u0000\u0000\u0248\u0249\u0005\u0099\u0000\u0000\u0249"+
		"\u024a\u0005\u0116\u0000\u0000\u024a\u024b\u0003\u00ceg\u0000\u024b\u024c"+
		"\u0005\u00e7\u0000\u0000\u024c\u024d\u0005\u00c7\u0000\u0000\u024d\u024e"+
		"\u0003\u0018\f\u0000\u024e\u0405\u0001\u0000\u0000\u0000\u024f\u0250\u0005"+
		"N\u0000\u0000\u0250\u0253\u0005\u0116\u0000\u0000\u0251\u0252\u0005o\u0000"+
		"\u0000\u0252\u0254\u0005X\u0000\u0000\u0253\u0251\u0001\u0000\u0000\u0000"+
		"\u0253\u0254\u0001\u0000\u0000\u0000\u0254\u0255\u0001\u0000\u0000\u0000"+
		"\u0255\u0405\u0003\u00ceg\u0000\u0256\u0257\u0005\u0017\u0000\u0000\u0257"+
		"\u0258\u0005\u0116\u0000\u0000\u0258\u0259\u0003\u00ceg\u0000\u0259\u025a"+
		"\u0005\u00ce\u0000\u0000\u025a\u025b\u0005\u00f9\u0000\u0000\u025b\u025c"+
		"\u0003\u00ceg\u0000\u025c\u0405\u0001\u0000\u0000\u0000\u025d\u025e\u0005"+
		"\u0017\u0000\u0000\u025e\u025f\u0005\u0116\u0000\u0000\u025f\u0260\u0003"+
		"\u00ceg\u0000\u0260\u0261\u0005\u00e7\u0000\u0000\u0261\u0262\u0005\u001f"+
		"\u0000\u0000\u0262\u0263\u0003\u00d6k\u0000\u0263\u0405\u0001\u0000\u0000"+
		"\u0000\u0264\u0265\u0005$\u0000\u0000\u0265\u0266\u0003\u00ceg\u0000\u0266"+
		"\u026f\u0005\u0002\u0000\u0000\u0267\u026c\u0003\u00c6c\u0000\u0268\u0269"+
		"\u0005\u0004\u0000\u0000\u0269\u026b\u0003\u00c6c\u0000\u026a\u0268\u0001"+
		"\u0000\u0000\u0000\u026b\u026e\u0001\u0000\u0000\u0000\u026c\u026a\u0001"+
		"\u0000\u0000\u0000\u026c\u026d\u0001\u0000\u0000\u0000\u026d\u0270\u0001"+
		"\u0000\u0000\u0000\u026e\u026c\u0001\u0000\u0000\u0000\u026f\u0267\u0001"+
		"\u0000\u0000\u0000\u026f\u0270\u0001\u0000\u0000\u0000\u0270\u0271\u0001"+
		"\u0000\u0000\u0000\u0271\u0272\u0005\u0003\u0000\u0000\u0272\u0405\u0001"+
		"\u0000\u0000\u0000\u0273\u0274\u00053\u0000\u0000\u0274\u0275\u0005\u00d7"+
		"\u0000\u0000\u0275\u0279\u0003\u00dam\u0000\u0276\u0277\u0005\u011a\u0000"+
		"\u0000\u0277\u0278\u0005\u0014\u0000\u0000\u0278\u027a\u0003\u00d4j\u0000"+
		"\u0279\u0276\u0001\u0000\u0000\u0000\u0279\u027a\u0001\u0000\u0000\u0000"+
		"\u027a\u027d\u0001\u0000\u0000\u0000\u027b\u027c\u0005q\u0000\u0000\u027c"+
		"\u027e\u0003\u00dam\u0000\u027d\u027b\u0001\u0000\u0000\u0000\u027d\u027e"+
		"\u0001\u0000\u0000\u0000\u027e\u0405\u0001\u0000\u0000\u0000\u027f\u0280"+
		"\u0005N\u0000\u0000\u0280\u0281\u0005\u00d7\u0000\u0000\u0281\u0284\u0003"+
		"\u00dam\u0000\u0282\u0283\u0005q\u0000\u0000\u0283\u0285\u0003\u00dam"+
		"\u0000\u0284\u0282\u0001\u0000\u0000\u0000\u0284\u0285\u0001\u0000\u0000"+
		"\u0000\u0285\u0405\u0001\u0000\u0000\u0000\u0286\u0287\u0005f\u0000\u0000"+
		"\u0287\u0288\u0003\u00d8l\u0000\u0288\u0289\u0005\u00f9\u0000\u0000\u0289"+
		"\u028e\u0003\u00d6k\u0000\u028a\u028b\u0005\u0004\u0000\u0000\u028b\u028d"+
		"\u0003\u00d6k\u0000\u028c\u028a\u0001\u0000\u0000\u0000\u028d\u0290\u0001"+
		"\u0000\u0000\u0000\u028e\u028c\u0001\u0000\u0000\u0000\u028e\u028f\u0001"+
		"\u0000\u0000\u0000\u028f\u0294\u0001\u0000\u0000\u0000\u0290\u028e\u0001"+
		"\u0000\u0000\u0000\u0291\u0292\u0005\u011a\u0000\u0000\u0292\u0293\u0005"+
		"\u0014\u0000\u0000\u0293\u0295\u0005\u00b2\u0000\u0000\u0294\u0291\u0001"+
		"\u0000\u0000\u0000\u0294\u0295\u0001\u0000\u0000\u0000\u0295\u0299\u0001"+
		"\u0000\u0000\u0000\u0296\u0297\u0005g\u0000\u0000\u0297\u0298\u0005#\u0000"+
		"\u0000\u0298\u029a\u0003\u00d4j\u0000\u0299\u0296\u0001\u0000\u0000\u0000"+
		"\u0299\u029a\u0001\u0000\u0000\u0000\u029a\u029d\u0001\u0000\u0000\u0000"+
		"\u029b\u029c\u0005q\u0000\u0000\u029c\u029e\u0003\u00dam\u0000\u029d\u029b"+
		"\u0001\u0000\u0000\u0000\u029d\u029e\u0001\u0000\u0000\u0000\u029e\u0405"+
		"\u0001\u0000\u0000\u0000\u029f\u02a3\u0005\u00d5\u0000\u0000\u02a0\u02a1"+
		"\u0005\u0014\u0000\u0000\u02a1\u02a2\u0005\u00b2\u0000\u0000\u02a2\u02a4"+
		"\u0005a\u0000\u0000\u02a3\u02a0\u0001\u0000\u0000\u0000\u02a3\u02a4\u0001"+
		"\u0000\u0000\u0000\u02a4\u02a5\u0001\u0000\u0000\u0000\u02a5\u02a6\u0003"+
		"\u00d8l\u0000\u02a6\u02a7\u0005c\u0000\u0000\u02a7\u02ac\u0003\u00d6k"+
		"\u0000\u02a8\u02a9\u0005\u0004\u0000\u0000\u02a9\u02ab\u0003\u00d6k\u0000"+
		"\u02aa\u02a8\u0001\u0000\u0000\u0000\u02ab\u02ae\u0001\u0000\u0000\u0000"+
		"\u02ac\u02aa\u0001\u0000\u0000\u0000\u02ac\u02ad\u0001\u0000\u0000\u0000"+
		"\u02ad\u02b2\u0001\u0000\u0000\u0000\u02ae\u02ac\u0001\u0000\u0000\u0000"+
		"\u02af\u02b0\u0005g\u0000\u0000\u02b0\u02b1\u0005#\u0000\u0000\u02b1\u02b3"+
		"\u0003\u00d4j\u0000\u02b2\u02af\u0001\u0000\u0000\u0000\u02b2\u02b3\u0001"+
		"\u0000\u0000\u0000\u02b3\u02b6\u0001\u0000\u0000\u0000\u02b4\u02b5\u0005"+
		"q\u0000\u0000\u02b5\u02b7\u0003\u00dam\u0000\u02b6\u02b4\u0001\u0000\u0000"+
		"\u0000\u02b6\u02b7\u0001\u0000\u0000\u0000\u02b7\u0405\u0001\u0000\u0000"+
		"\u0000\u02b8\u02b9\u0005\u00e7\u0000\u0000\u02b9\u02bd\u0005\u00d7\u0000"+
		"\u0000\u02ba\u02be\u0005\u0016\u0000\u0000\u02bb\u02be\u0005\u00a5\u0000"+
		"\u0000\u02bc\u02be\u0003\u00dam\u0000\u02bd\u02ba\u0001\u0000\u0000\u0000"+
		"\u02bd\u02bb\u0001\u0000\u0000\u0000\u02bd\u02bc\u0001\u0000\u0000\u0000"+
		"\u02be\u02c1\u0001\u0000\u0000\u0000\u02bf\u02c0\u0005q\u0000\u0000\u02c0"+
		"\u02c2\u0003\u00dam\u0000\u02c1\u02bf\u0001\u0000\u0000\u0000\u02c1\u02c2"+
		"\u0001\u0000\u0000\u0000\u02c2\u0405\u0001\u0000\u0000\u0000\u02c3\u02ce"+
		"\u0005f\u0000\u0000\u02c4\u02c9\u0003\u00ccf\u0000\u02c5\u02c6\u0005\u0004"+
		"\u0000\u0000\u02c6\u02c8\u0003\u00ccf\u0000\u02c7\u02c5\u0001\u0000\u0000"+
		"\u0000\u02c8\u02cb\u0001\u0000\u0000\u0000\u02c9\u02c7\u0001\u0000\u0000"+
		"\u0000\u02c9\u02ca\u0001\u0000\u0000\u0000\u02ca\u02cf\u0001\u0000\u0000"+
		"\u0000\u02cb\u02c9\u0001\u0000\u0000\u0000\u02cc\u02cd\u0005\u0016\u0000"+
		"\u0000\u02cd\u02cf\u0005\u00c6\u0000\u0000\u02ce\u02c4\u0001\u0000\u0000"+
		"\u0000\u02ce\u02cc\u0001\u0000\u0000\u0000\u02cf\u02d0\u0001\u0000\u0000"+
		"\u0000\u02d0\u02d2\u0005\u00af\u0000\u0000\u02d1\u02d3\u0007\u0002\u0000"+
		"\u0000\u02d2\u02d1\u0001\u0000\u0000\u0000\u02d2\u02d3\u0001\u0000\u0000"+
		"\u0000\u02d3\u02d4\u0001\u0000\u0000\u0000\u02d4\u02d5\u0003\u00ceg\u0000"+
		"\u02d5\u02d6\u0005\u00f9\u0000\u0000\u02d6\u02da\u0003\u00d6k\u0000\u02d7"+
		"\u02d8\u0005\u011a\u0000\u0000\u02d8\u02d9\u0005f\u0000\u0000\u02d9\u02db"+
		"\u0005\u00b2\u0000\u0000\u02da\u02d7\u0001\u0000\u0000\u0000\u02da\u02db"+
		"\u0001\u0000\u0000\u0000\u02db\u0405\u0001\u0000\u0000\u0000\u02dc\u02e7"+
		"\u0005G\u0000\u0000\u02dd\u02e2\u0003\u00ccf\u0000\u02de\u02df\u0005\u0004"+
		"\u0000\u0000\u02df\u02e1\u0003\u00ccf\u0000\u02e0\u02de\u0001\u0000\u0000"+
		"\u0000\u02e1\u02e4\u0001\u0000\u0000\u0000\u02e2\u02e0\u0001\u0000\u0000"+
		"\u0000\u02e2\u02e3\u0001\u0000\u0000\u0000\u02e3\u02e8\u0001\u0000\u0000"+
		"\u0000\u02e4\u02e2\u0001\u0000\u0000\u0000\u02e5\u02e6\u0005\u0016\u0000"+
		"\u0000\u02e6\u02e8\u0005\u00c6\u0000\u0000\u02e7\u02dd\u0001\u0000\u0000"+
		"\u0000\u02e7\u02e5\u0001\u0000\u0000\u0000\u02e8\u02e9\u0001\u0000\u0000"+
		"\u0000\u02e9\u02eb\u0005\u00af\u0000\u0000\u02ea\u02ec\u0007\u0002\u0000"+
		"\u0000\u02eb\u02ea\u0001\u0000\u0000\u0000\u02eb\u02ec\u0001\u0000\u0000"+
		"\u0000\u02ec\u02ed\u0001\u0000\u0000\u0000\u02ed\u02ee\u0003\u00ceg\u0000"+
		"\u02ee\u02ef\u0005\u00f9\u0000\u0000\u02ef\u02f0\u0003\u00d6k\u0000\u02f0"+
		"\u0405\u0001\u0000\u0000\u0000\u02f1\u02f5\u0005\u00d5\u0000\u0000\u02f2"+
		"\u02f3\u0005f\u0000\u0000\u02f3\u02f4\u0005\u00b2\u0000\u0000\u02f4\u02f6"+
		"\u0005a\u0000\u0000\u02f5\u02f2\u0001\u0000\u0000\u0000\u02f5\u02f6\u0001"+
		"\u0000\u0000\u0000\u02f6\u0301\u0001\u0000\u0000\u0000\u02f7\u02fc\u0003"+
		"\u00ccf\u0000\u02f8\u02f9\u0005\u0004\u0000\u0000\u02f9\u02fb\u0003\u00cc"+
		"f\u0000\u02fa\u02f8\u0001\u0000\u0000\u0000\u02fb\u02fe\u0001\u0000\u0000"+
		"\u0000\u02fc\u02fa\u0001\u0000\u0000\u0000\u02fc\u02fd\u0001\u0000\u0000"+
		"\u0000\u02fd\u0302\u0001\u0000\u0000\u0000\u02fe\u02fc\u0001\u0000\u0000"+
		"\u0000\u02ff\u0300\u0005\u0016\u0000\u0000\u0300\u0302\u0005\u00c6\u0000"+
		"\u0000\u0301\u02f7\u0001\u0000\u0000\u0000\u0301\u02ff\u0001\u0000\u0000"+
		"\u0000\u0302\u0303\u0001\u0000\u0000\u0000\u0303\u0305\u0005\u00af\u0000"+
		"\u0000\u0304\u0306\u0007\u0002\u0000\u0000\u0305\u0304\u0001\u0000\u0000"+
		"\u0000\u0305\u0306\u0001\u0000\u0000\u0000\u0306\u0307\u0001\u0000\u0000"+
		"\u0000\u0307\u0308\u0003\u00ceg\u0000\u0308\u0309\u0005c\u0000\u0000\u0309"+
		"\u030a\u0003\u00d6k\u0000\u030a\u0405\u0001\u0000\u0000\u0000\u030b\u030c"+
		"\u0005\u00e9\u0000\u0000\u030c\u0312\u0005h\u0000\u0000\u030d\u030f\u0005"+
		"\u00af\u0000\u0000\u030e\u0310\u0005\u00f0\u0000\u0000\u030f\u030e\u0001"+
		"\u0000\u0000\u0000\u030f\u0310\u0001\u0000\u0000\u0000\u0310\u0311\u0001"+
		"\u0000\u0000\u0000\u0311\u0313\u0003\u00ceg\u0000\u0312\u030d\u0001\u0000"+
		"\u0000\u0000\u0312\u0313\u0001\u0000\u0000\u0000\u0313\u0405\u0001\u0000"+
		"\u0000\u0000\u0314\u0320\u0005Y\u0000\u0000\u0315\u0316\u0005\u0002\u0000"+
		"\u0000\u0316\u031b\u0003\u00c0`\u0000\u0317\u0318\u0005\u0004\u0000\u0000"+
		"\u0318\u031a\u0003\u00c0`\u0000\u0319\u0317\u0001\u0000\u0000\u0000\u031a"+
		"\u031d\u0001\u0000\u0000\u0000\u031b\u0319\u0001\u0000\u0000\u0000\u031b"+
		"\u031c\u0001\u0000\u0000\u0000\u031c\u031e\u0001\u0000\u0000\u0000\u031d"+
		"\u031b\u0001\u0000\u0000\u0000\u031e\u031f\u0005\u0003\u0000\u0000\u031f"+
		"\u0321\u0001\u0000\u0000\u0000\u0320\u0315\u0001\u0000\u0000\u0000\u0320"+
		"\u0321\u0001\u0000\u0000\u0000\u0321\u0322\u0001\u0000\u0000\u0000\u0322"+
		"\u0405\u0003\n\u0005\u0000\u0323\u0324\u0005Y\u0000\u0000\u0324\u0326"+
		"\u0005\u0018\u0000\u0000\u0325\u0327\u0005\u0114\u0000\u0000\u0326\u0325"+
		"\u0001\u0000\u0000\u0000\u0326\u0327\u0001\u0000\u0000\u0000\u0327\u0328"+
		"\u0001\u0000\u0000\u0000\u0328\u0405\u0003\n\u0005\u0000\u0329\u032a\u0005"+
		"\u00e9\u0000\u0000\u032a\u032b\u00053\u0000\u0000\u032b\u032c\u0005\u00f0"+
		"\u0000\u0000\u032c\u0405\u0003\u00ceg\u0000\u032d\u032e\u0005\u00e9\u0000"+
		"\u0000\u032e\u032f\u00053\u0000\u0000\u032f\u0330\u0005\u00df\u0000\u0000"+
		"\u0330\u0405\u0003\u00ceg\u0000\u0331\u0332\u0005\u00e9\u0000\u0000\u0332"+
		"\u0333\u00053\u0000\u0000\u0333\u0334\u0005\u0116\u0000\u0000\u0334\u0405"+
		"\u0003\u00ceg\u0000\u0335\u0336\u0005\u00e9\u0000\u0000\u0336\u0337\u0005"+
		"3\u0000\u0000\u0337\u0338\u0005\u0099\u0000\u0000\u0338\u0339\u0005\u0116"+
		"\u0000\u0000\u0339\u0405\u0003\u00ceg\u0000\u033a\u033b\u0005\u00e9\u0000"+
		"\u0000\u033b\u033e\u0005\u00f1\u0000\u0000\u033c\u033d\u0007\u0003\u0000"+
		"\u0000\u033d\u033f\u0003\u00ceg\u0000\u033e\u033c\u0001\u0000\u0000\u0000"+
		"\u033e\u033f\u0001\u0000\u0000\u0000\u033f\u0346\u0001\u0000\u0000\u0000"+
		"\u0340\u0341\u0005\u008d\u0000\u0000\u0341\u0344\u0003\u0094J\u0000\u0342"+
		"\u0343\u0005T\u0000\u0000\u0343\u0345\u0003\u0094J\u0000\u0344\u0342\u0001"+
		"\u0000\u0000\u0000\u0344\u0345\u0001\u0000\u0000\u0000\u0345\u0347\u0001"+
		"\u0000\u0000\u0000\u0346\u0340\u0001\u0000\u0000\u0000\u0346\u0347\u0001"+
		"\u0000\u0000\u0000\u0347\u0405\u0001\u0000\u0000\u0000\u0348\u0349\u0005"+
		"\u00e9\u0000\u0000\u0349\u034c\u0005\u00e0\u0000\u0000\u034a\u034b\u0007"+
		"\u0003\u0000\u0000\u034b\u034d\u0003\u00dam\u0000\u034c\u034a\u0001\u0000"+
		"\u0000\u0000\u034c\u034d\u0001\u0000\u0000\u0000\u034d\u0354\u0001\u0000"+
		"\u0000\u0000\u034e\u034f\u0005\u008d\u0000\u0000\u034f\u0352\u0003\u0094"+
		"J\u0000\u0350\u0351\u0005T\u0000\u0000\u0351\u0353\u0003\u0094J\u0000"+
		"\u0352\u0350\u0001\u0000\u0000\u0000\u0352\u0353\u0001\u0000\u0000\u0000"+
		"\u0353\u0355\u0001\u0000\u0000\u0000\u0354\u034e\u0001\u0000\u0000\u0000"+
		"\u0354\u0355\u0001\u0000\u0000\u0000\u0355\u0405\u0001\u0000\u0000\u0000"+
		"\u0356\u0357\u0005\u00e9\u0000\u0000\u0357\u035e\u0005)\u0000\u0000\u0358"+
		"\u0359\u0005\u008d\u0000\u0000\u0359\u035c\u0003\u0094J\u0000\u035a\u035b"+
		"\u0005T\u0000\u0000\u035b\u035d\u0003\u0094J\u0000\u035c\u035a\u0001\u0000"+
		"\u0000\u0000\u035c\u035d\u0001\u0000\u0000\u0000\u035d\u035f\u0001\u0000"+
		"\u0000\u0000\u035e\u0358\u0001\u0000\u0000\u0000\u035e\u035f\u0001\u0000"+
		"\u0000\u0000\u035f\u0405\u0001\u0000\u0000\u0000\u0360\u0361\u0005\u00e9"+
		"\u0000\u0000\u0361\u0362\u0005+\u0000\u0000\u0362\u0364\u0007\u0003\u0000"+
		"\u0000\u0363\u0365\u0003\u00ceg\u0000\u0364\u0363\u0001\u0000\u0000\u0000"+
		"\u0364\u0365\u0001\u0000\u0000\u0000\u0365\u036c\u0001\u0000\u0000\u0000"+
		"\u0366\u0367\u0005\u008d\u0000\u0000\u0367\u036a\u0003\u0094J\u0000\u0368"+
		"\u0369\u0005T\u0000\u0000\u0369\u036b\u0003\u0094J\u0000\u036a\u0368\u0001"+
		"\u0000\u0000\u0000\u036a\u036b\u0001\u0000\u0000\u0000\u036b\u036d\u0001"+
		"\u0000\u0000\u0000\u036c\u0366\u0001\u0000\u0000\u0000\u036c\u036d\u0001"+
		"\u0000\u0000\u0000\u036d\u0405\u0001\u0000\u0000\u0000\u036e\u036f\u0005"+
		"\u00e9\u0000\u0000\u036f\u0370\u0005\u00ec\u0000\u0000\u0370\u0371\u0005"+
		"a\u0000\u0000\u0371\u0405\u0003\u00ceg\u0000\u0372\u0373\u0005\u00e9\u0000"+
		"\u0000\u0373\u0374\u0005\u00ec\u0000\u0000\u0374\u0375\u0005a\u0000\u0000"+
		"\u0375\u0376\u0005\u0002\u0000\u0000\u0376\u0377\u0003\f\u0006\u0000\u0377"+
		"\u0378\u0005\u0003\u0000\u0000\u0378\u0405\u0001\u0000\u0000\u0000\u0379"+
		"\u037b\u0005\u00e9\u0000\u0000\u037a\u037c\u00056\u0000\u0000\u037b\u037a"+
		"\u0001\u0000\u0000\u0000\u037b\u037c\u0001\u0000\u0000\u0000\u037c\u037d"+
		"\u0001\u0000\u0000\u0000\u037d\u0380\u0005\u00d8\u0000\u0000\u037e\u037f"+
		"\u0007\u0003\u0000\u0000\u037f\u0381\u0003\u00dam\u0000\u0380\u037e\u0001"+
		"\u0000\u0000\u0000\u0380\u0381\u0001\u0000\u0000\u0000\u0381\u0405\u0001"+
		"\u0000\u0000\u0000\u0382\u0383\u0005\u00e9\u0000\u0000\u0383\u0384\u0005"+
		"\u00d7\u0000\u0000\u0384\u0387\u0005h\u0000\u0000\u0385\u0386\u0007\u0003"+
		"\u0000\u0000\u0386\u0388\u0003\u00dam\u0000\u0387\u0385\u0001\u0000\u0000"+
		"\u0000\u0387\u0388\u0001\u0000\u0000\u0000\u0388\u0405\u0001\u0000\u0000"+
		"\u0000\u0389\u038a\u0005I\u0000\u0000\u038a\u0405\u0003\u00ceg\u0000\u038b"+
		"\u038c\u0005H\u0000\u0000\u038c\u0405\u0003\u00ceg\u0000\u038d\u038e\u0005"+
		"\u00e9\u0000\u0000\u038e\u0395\u0005e\u0000\u0000\u038f\u0390\u0005\u008d"+
		"\u0000\u0000\u0390\u0393\u0003\u0094J\u0000\u0391\u0392\u0005T\u0000\u0000"+
		"\u0392\u0394\u0003\u0094J\u0000\u0393\u0391\u0001\u0000\u0000\u0000\u0393"+
		"\u0394\u0001\u0000\u0000\u0000\u0394\u0396\u0001\u0000\u0000\u0000\u0395"+
		"\u038f\u0001\u0000\u0000\u0000\u0395\u0396\u0001\u0000\u0000\u0000\u0396"+
		"\u0405\u0001\u0000\u0000\u0000\u0397\u0398\u0005\u00e9\u0000\u0000\u0398"+
		"\u039f\u0005\u00e6\u0000\u0000\u0399\u039a\u0005\u008d\u0000\u0000\u039a"+
		"\u039d\u0003\u0094J\u0000\u039b\u039c\u0005T\u0000\u0000\u039c\u039e\u0003"+
		"\u0094J\u0000\u039d\u039b\u0001\u0000\u0000\u0000\u039d\u039e\u0001\u0000"+
		"\u0000\u0000\u039e\u03a0\u0001\u0000\u0000\u0000\u039f\u0399\u0001\u0000"+
		"\u0000\u0000\u039f\u03a0\u0001\u0000\u0000\u0000\u03a0\u0405\u0001\u0000"+
		"\u0000\u0000\u03a1\u03a2\u0005\u00e7\u0000\u0000\u03a2\u03a3\u0005\u00e6"+
		"\u0000\u0000\u03a3\u03a4\u0003\u00ceg\u0000\u03a4\u03a5\u0005\u0122\u0000"+
		"\u0000\u03a5\u03a6\u0003t:\u0000\u03a6\u0405\u0001\u0000\u0000\u0000\u03a7"+
		"\u03a8\u0005\u00d1\u0000\u0000\u03a8\u03a9\u0005\u00e6\u0000\u0000\u03a9"+
		"\u0405\u0003\u00ceg\u0000\u03aa\u03ab\u0005\u00eb\u0000\u0000\u03ab\u03b4"+
		"\u0005\u00fb\u0000\u0000\u03ac\u03b1\u0003\u00c2a\u0000\u03ad\u03ae\u0005"+
		"\u0004\u0000\u0000\u03ae\u03b0\u0003\u00c2a\u0000\u03af\u03ad\u0001\u0000"+
		"\u0000\u0000\u03b0\u03b3\u0001\u0000\u0000\u0000\u03b1\u03af\u0001\u0000"+
		"\u0000\u0000\u03b1\u03b2\u0001\u0000\u0000\u0000\u03b2\u03b5\u0001\u0000"+
		"\u0000\u0000\u03b3\u03b1\u0001\u0000\u0000\u0000\u03b4\u03ac\u0001\u0000"+
		"\u0000\u0000\u03b4\u03b5\u0001\u0000\u0000\u0000\u03b5\u0405\u0001\u0000"+
		"\u0000\u0000\u03b6\u03b8\u0005-\u0000\u0000\u03b7\u03b9\u0005\u011d\u0000"+
		"\u0000\u03b8\u03b7\u0001\u0000\u0000\u0000\u03b8\u03b9\u0001\u0000\u0000"+
		"\u0000\u03b9\u0405\u0001\u0000\u0000\u0000\u03ba\u03bc\u0005\u00d9\u0000"+
		"\u0000\u03bb\u03bd\u0005\u011d\u0000\u0000\u03bc\u03bb\u0001\u0000\u0000"+
		"\u0000\u03bc\u03bd\u0001\u0000\u0000\u0000\u03bd\u0405\u0001\u0000\u0000"+
		"\u0000\u03be\u03bf\u0005\u00c5\u0000\u0000\u03bf\u03c0\u0003\u00dam\u0000"+
		"\u03c0\u03c1\u0005c\u0000\u0000\u03c1\u03c2\u0003\n\u0005\u0000\u03c2"+
		"\u0405\u0001\u0000\u0000\u0000\u03c3\u03c4\u0005B\u0000\u0000\u03c4\u03c5"+
		"\u0005\u00c5\u0000\u0000\u03c5\u0405\u0003\u00dam\u0000\u03c6\u03c7\u0005"+
		"W\u0000\u0000\u03c7\u03d1\u0003\u00dam\u0000\u03c8\u03c9\u0005\u010d\u0000"+
		"\u0000\u03c9\u03ce\u0003t:\u0000\u03ca\u03cb\u0005\u0004\u0000\u0000\u03cb"+
		"\u03cd\u0003t:\u0000\u03cc\u03ca\u0001\u0000\u0000\u0000\u03cd\u03d0\u0001"+
		"\u0000\u0000\u0000\u03ce\u03cc\u0001\u0000\u0000\u0000\u03ce\u03cf\u0001"+
		"\u0000\u0000\u0000\u03cf\u03d2\u0001\u0000\u0000\u0000\u03d0\u03ce\u0001"+
		"\u0000\u0000\u0000\u03d1\u03c8\u0001\u0000\u0000\u0000\u03d1\u03d2\u0001"+
		"\u0000\u0000\u0000\u03d2\u0405\u0001\u0000\u0000\u0000\u03d3\u03d4\u0005"+
		"I\u0000\u0000\u03d4\u03d5\u0005u\u0000\u0000\u03d5\u0405\u0003\u00dam"+
		"\u0000\u03d6\u03d7\u0005I\u0000\u0000\u03d7\u03d8\u0005\u00b7\u0000\u0000"+
		"\u03d8\u0405\u0003\u00dam\u0000\u03d9\u03da\u0005\u00e7\u0000\u0000\u03da"+
		"\u03db\u0005\u00be\u0000\u0000\u03db\u0405\u0003\u00cae\u0000\u03dc\u03dd"+
		"\u0005\u00e7\u0000\u0000\u03dd\u03de\u0005\u00f7\u0000\u0000\u03de\u03e1"+
		"\u0005\u0121\u0000\u0000\u03df\u03e2\u0005\u0090\u0000\u0000\u03e0\u03e2"+
		"\u0003t:\u0000\u03e1\u03df\u0001\u0000\u0000\u0000\u03e1\u03e0\u0001\u0000"+
		"\u0000\u0000\u03e2\u0405\u0001\u0000\u0000\u0000\u03e3\u03e4\u0005\u010a"+
		"\u0000\u0000\u03e4\u03e5\u0003\u00ceg\u0000\u03e5\u03e6\u0005\u00e7\u0000"+
		"\u0000\u03e6\u03eb\u0003\u00be_\u0000\u03e7\u03e8\u0005\u0004\u0000\u0000"+
		"\u03e8\u03ea\u0003\u00be_\u0000\u03e9\u03e7\u0001\u0000\u0000\u0000\u03ea"+
		"\u03ed\u0001\u0000\u0000\u0000\u03eb\u03e9\u0001\u0000\u0000\u0000\u03eb"+
		"\u03ec\u0001\u0000\u0000\u0000\u03ec\u03f0\u0001\u0000\u0000\u0000\u03ed"+
		"\u03eb\u0001\u0000\u0000\u0000\u03ee\u03ef\u0005\u0118\u0000\u0000\u03ef"+
		"\u03f1\u0003v;\u0000\u03f0\u03ee\u0001\u0000\u0000\u0000\u03f0\u03f1\u0001"+
		"\u0000\u0000\u0000\u03f1\u0405\u0001\u0000\u0000\u0000\u03f2\u03f3\u0005"+
		"\u009b\u0000\u0000\u03f3\u03f4\u0005y\u0000\u0000\u03f4\u03f9\u0003\u00ce"+
		"g\u0000\u03f5\u03f7\u0005\u001c\u0000\u0000\u03f6\u03f5\u0001\u0000\u0000"+
		"\u0000\u03f6\u03f7\u0001\u0000\u0000\u0000\u03f7\u03f8\u0001\u0000\u0000"+
		"\u0000\u03f8\u03fa\u0003\u00dam\u0000\u03f9\u03f6\u0001\u0000\u0000\u0000"+
		"\u03f9\u03fa\u0001\u0000\u0000\u0000\u03fa\u03fb\u0001\u0000\u0000\u0000"+
		"\u03fb\u03fc\u0005\u010d\u0000\u0000\u03fc\u03fd\u0003B!\u0000\u03fd\u03fe"+
		"\u0005\u00af\u0000\u0000\u03fe\u0400\u0003t:\u0000\u03ff\u0401\u0003\u00ae"+
		"W\u0000\u0400\u03ff\u0001\u0000\u0000\u0000\u0401\u0402\u0001\u0000\u0000"+
		"\u0000\u0402\u0400\u0001\u0000\u0000\u0000\u0402\u0403\u0001\u0000\u0000"+
		"\u0000\u0403\u0405\u0001\u0000\u0000\u0000\u0404\u00ef\u0001\u0000\u0000"+
		"\u0000\u0404\u00f0\u0001\u0000\u0000\u0000\u0404\u00f2\u0001\u0000\u0000"+
		"\u0000\u0404\u00f7\u0001\u0000\u0000\u0000\u0404\u0107\u0001\u0000\u0000"+
		"\u0000\u0404\u0117\u0001\u0000\u0000\u0000\u0404\u0121\u0001\u0000\u0000"+
		"\u0000\u0404\u0128\u0001\u0000\u0000\u0000\u0404\u012f\u0001\u0000\u0000"+
		"\u0000\u0404\u0151\u0001\u0000\u0000\u0000\u0404\u016b\u0001\u0000\u0000"+
		"\u0000\u0404\u0172\u0001\u0000\u0000\u0000\u0404\u017a\u0001\u0000\u0000"+
		"\u0000\u0404\u0181\u0001\u0000\u0000\u0000\u0404\u0184\u0001\u0000\u0000"+
		"\u0000\u0404\u018d\u0001\u0000\u0000\u0000\u0404\u0196\u0001\u0000\u0000"+
		"\u0000\u0404\u019f\u0001\u0000\u0000\u0000\u0404\u01aa\u0001\u0000\u0000"+
		"\u0000\u0404\u01ba\u0001\u0000\u0000\u0000\u0404\u01cb\u0001\u0000\u0000"+
		"\u0000\u0404\u01da\u0001\u0000\u0000\u0000\u0404\u01e1\u0001\u0000\u0000"+
		"\u0000\u0404\u01e8\u0001\u0000\u0000\u0000\u0404\u01ff\u0001\u0000\u0000"+
		"\u0000\u0404\u0205\u0001\u0000\u0000\u0000\u0404\u021d\u0001\u0000\u0000"+
		"\u0000\u0404\u022f\u0001\u0000\u0000\u0000\u0404\u0233\u0001\u0000\u0000"+
		"\u0000\u0404\u023b\u0001\u0000\u0000\u0000\u0404\u0247\u0001\u0000\u0000"+
		"\u0000\u0404\u024f\u0001\u0000\u0000\u0000\u0404\u0256\u0001\u0000\u0000"+
		"\u0000\u0404\u025d\u0001\u0000\u0000\u0000\u0404\u0264\u0001\u0000\u0000"+
		"\u0000\u0404\u0273\u0001\u0000\u0000\u0000\u0404\u027f\u0001\u0000\u0000"+
		"\u0000\u0404\u0286\u0001\u0000\u0000\u0000\u0404\u029f\u0001\u0000\u0000"+
		"\u0000\u0404\u02b8\u0001\u0000\u0000\u0000\u0404\u02c3\u0001\u0000\u0000"+
		"\u0000\u0404\u02dc\u0001\u0000\u0000\u0000\u0404\u02f1\u0001\u0000\u0000"+
		"\u0000\u0404\u030b\u0001\u0000\u0000\u0000\u0404\u0314\u0001\u0000\u0000"+
		"\u0000\u0404\u0323\u0001\u0000\u0000\u0000\u0404\u0329\u0001\u0000\u0000"+
		"\u0000\u0404\u032d\u0001\u0000\u0000\u0000\u0404\u0331\u0001\u0000\u0000"+
		"\u0000\u0404\u0335\u0001\u0000\u0000\u0000\u0404\u033a\u0001\u0000\u0000"+
		"\u0000\u0404\u0348\u0001\u0000\u0000\u0000\u0404\u0356\u0001\u0000\u0000"+
		"\u0000\u0404\u0360\u0001\u0000\u0000\u0000\u0404\u036e\u0001\u0000\u0000"+
		"\u0000\u0404\u0372\u0001\u0000\u0000\u0000\u0404\u0379\u0001\u0000\u0000"+
		"\u0000\u0404\u0382\u0001\u0000\u0000\u0000\u0404\u0389\u0001\u0000\u0000"+
		"\u0000\u0404\u038b\u0001\u0000\u0000\u0000\u0404\u038d\u0001\u0000\u0000"+
		"\u0000\u0404\u0397\u0001\u0000\u0000\u0000\u0404\u03a1\u0001\u0000\u0000"+
		"\u0000\u0404\u03a7\u0001\u0000\u0000\u0000\u0404\u03aa\u0001\u0000\u0000"+
		"\u0000\u0404\u03b6\u0001\u0000\u0000\u0000\u0404\u03ba\u0001\u0000\u0000"+
		"\u0000\u0404\u03be\u0001\u0000\u0000\u0000\u0404\u03c3\u0001\u0000\u0000"+
		"\u0000\u0404\u03c6\u0001\u0000\u0000\u0000\u0404\u03d3\u0001\u0000\u0000"+
		"\u0000\u0404\u03d6\u0001\u0000\u0000\u0000\u0404\u03d9\u0001\u0000\u0000"+
		"\u0000\u0404\u03dc\u0001\u0000\u0000\u0000\u0404\u03e3\u0001\u0000\u0000"+
		"\u0000\u0404\u03f2\u0001\u0000\u0000\u0000\u0405\u000b\u0001\u0000\u0000"+
		"\u0000\u0406\u0408\u0003\u000e\u0007\u0000\u0407\u0406\u0001\u0000\u0000"+
		"\u0000\u0407\u0408\u0001\u0000\u0000\u0000\u0408\u0409\u0001\u0000\u0000"+
		"\u0000\u0409\u040a\u0003$\u0012\u0000\u040a\r\u0001\u0000\u0000\u0000"+
		"\u040b\u040d\u0005\u011a\u0000\u0000\u040c\u040e\u0005\u00cc\u0000\u0000"+
		"\u040d\u040c\u0001\u0000\u0000\u0000\u040d\u040e\u0001\u0000\u0000\u0000"+
		"\u040e\u040f\u0001\u0000\u0000\u0000\u040f\u0414\u0003<\u001e\u0000\u0410"+
		"\u0411\u0005\u0004\u0000\u0000\u0411\u0413\u0003<\u001e\u0000\u0412\u0410"+
		"\u0001\u0000\u0000\u0000\u0413\u0416\u0001\u0000\u0000\u0000\u0414\u0412"+
		"\u0001\u0000\u0000\u0000\u0414\u0415\u0001\u0000\u0000\u0000\u0415\u000f"+
		"\u0001\u0000\u0000\u0000\u0416\u0414\u0001\u0000\u0000\u0000\u0417\u041a"+
		"\u0003\u0012\t\u0000\u0418\u041a\u0003\u0014\n\u0000\u0419\u0417\u0001"+
		"\u0000\u0000\u0000\u0419\u0418\u0001\u0000\u0000\u0000\u041a\u0011\u0001"+
		"\u0000\u0000\u0000\u041b\u041c\u0003\u00dam\u0000\u041c\u041f\u0003\u00a4"+
		"R\u0000\u041d\u041e\u0005\u00a7\u0000\u0000\u041e\u0420\u0005\u00a8\u0000"+
		"\u0000\u041f\u041d\u0001\u0000\u0000\u0000\u041f\u0420\u0001\u0000\u0000"+
		"\u0000\u0420\u0423\u0001\u0000\u0000\u0000\u0421\u0422\u0005,\u0000\u0000"+
		"\u0422\u0424\u0003\u0094J\u0000\u0423\u0421\u0001\u0000\u0000\u0000\u0423"+
		"\u0424\u0001\u0000\u0000\u0000\u0424\u0427\u0001\u0000\u0000\u0000\u0425"+
		"\u0426\u0005\u011a\u0000\u0000\u0426\u0428\u0003\u0016\u000b\u0000\u0427"+
		"\u0425\u0001\u0000\u0000\u0000\u0427\u0428\u0001\u0000\u0000\u0000\u0428"+
		"\u0013\u0001\u0000\u0000\u0000\u0429\u042a\u0005\u008d\u0000\u0000\u042a"+
		"\u042d\u0003\u00ceg\u0000\u042b\u042c\u0007\u0004\u0000\u0000\u042c\u042e"+
		"\u0005\u00c7\u0000\u0000\u042d\u042b\u0001\u0000\u0000\u0000\u042d\u042e"+
		"\u0001\u0000\u0000\u0000\u042e\u0015\u0001\u0000\u0000\u0000\u042f\u0430"+
		"\u0005\u0002\u0000\u0000\u0430\u0431\u0003\u0018\f\u0000\u0431\u0432\u0005"+
		"\u0003\u0000\u0000\u0432\u0017\u0001\u0000\u0000\u0000\u0433\u0438\u0003"+
		"\u001a\r\u0000\u0434\u0435\u0005\u0004\u0000\u0000\u0435\u0437\u0003\u001a"+
		"\r\u0000\u0436\u0434\u0001\u0000\u0000\u0000\u0437\u043a\u0001\u0000\u0000"+
		"\u0000\u0438\u0436\u0001\u0000\u0000\u0000\u0438\u0439\u0001\u0000\u0000"+
		"\u0000\u0439\u0019\u0001\u0000\u0000\u0000\u043a\u0438\u0001\u0000\u0000"+
		"\u0000\u043b\u043c\u0003\u00dam\u0000\u043c\u043d\u0005\u0122\u0000\u0000"+
		"\u043d\u043e\u0003\u001c\u000e\u0000\u043e\u001b\u0001\u0000\u0000\u0000"+
		"\u043f\u0442\u0005C\u0000\u0000\u0440\u0442\u0003t:\u0000\u0441\u043f"+
		"\u0001\u0000\u0000\u0000\u0441\u0440\u0001\u0000\u0000\u0000\u0442\u001d"+
		"\u0001\u0000\u0000\u0000\u0443\u0444\u0005\u0002\u0000\u0000\u0444\u0445"+
		"\u0003 \u0010\u0000\u0445\u0446\u0005\u0003\u0000\u0000\u0446\u001f\u0001"+
		"\u0000\u0000\u0000\u0447\u044c\u0003\"\u0011\u0000\u0448\u0449\u0005\u0004"+
		"\u0000\u0000\u0449\u044b\u0003\"\u0011\u0000\u044a\u0448\u0001\u0000\u0000"+
		"\u0000\u044b\u044e\u0001\u0000\u0000\u0000\u044c\u044a\u0001\u0000\u0000"+
		"\u0000\u044c\u044d\u0001\u0000\u0000\u0000\u044d!\u0001\u0000\u0000\u0000"+
		"\u044e\u044c\u0001\u0000\u0000\u0000\u044f\u0450\u0003\u0094J\u0000\u0450"+
		"\u0451\u0005\u0122\u0000\u0000\u0451\u0452\u0003t:\u0000\u0452#\u0001"+
		"\u0000\u0000\u0000\u0453\u045e\u0003*\u0015\u0000\u0454\u0455\u0005\u00b4"+
		"\u0000\u0000\u0455\u0456\u0005#\u0000\u0000\u0456\u045b\u0003.\u0017\u0000"+
		"\u0457\u0458\u0005\u0004\u0000\u0000\u0458\u045a\u0003.\u0017\u0000\u0459"+
		"\u0457\u0001\u0000\u0000\u0000\u045a\u045d\u0001\u0000\u0000\u0000\u045b"+
		"\u0459\u0001\u0000\u0000\u0000\u045b\u045c\u0001\u0000\u0000\u0000\u045c"+
		"\u045f\u0001\u0000\u0000\u0000\u045d\u045b\u0001\u0000\u0000\u0000\u045e"+
		"\u0454\u0001\u0000\u0000\u0000\u045e\u045f\u0001\u0000\u0000\u0000\u045f"+
		"\u0465\u0001\u0000\u0000\u0000\u0460\u0461\u0005\u00ad\u0000\u0000\u0461"+
		"\u0463\u0003(\u0014\u0000\u0462\u0464\u0007\u0005\u0000\u0000\u0463\u0462"+
		"\u0001\u0000\u0000\u0000\u0463\u0464\u0001\u0000\u0000\u0000\u0464\u0466"+
		"\u0001\u0000\u0000\u0000\u0465\u0460\u0001\u0000\u0000\u0000\u0465\u0466"+
		"\u0001\u0000\u0000\u0000\u0466\u0474\u0001\u0000\u0000\u0000\u0467\u0468"+
		"\u0005\u008e\u0000\u0000\u0468\u0475\u0003&\u0013\u0000\u0469\u046a\u0005"+
		"\\\u0000\u0000\u046a\u046c\u0007\u0006\u0000\u0000\u046b\u046d\u0003("+
		"\u0014\u0000\u046c\u046b\u0001\u0000\u0000\u0000\u046c\u046d\u0001\u0000"+
		"\u0000\u0000\u046d\u046e\u0001\u0000\u0000\u0000\u046e\u0472\u0007\u0005"+
		"\u0000\u0000\u046f\u0473\u0005\u00b1\u0000\u0000\u0470\u0471\u0005\u011a"+
		"\u0000\u0000\u0471\u0473\u0005\u00f6\u0000\u0000\u0472\u046f\u0001\u0000"+
		"\u0000\u0000\u0472\u0470\u0001\u0000\u0000\u0000\u0473\u0475\u0001\u0000"+
		"\u0000\u0000\u0474\u0467\u0001\u0000\u0000\u0000\u0474\u0469\u0001\u0000"+
		"\u0000\u0000\u0474\u0475\u0001\u0000\u0000\u0000\u0475%\u0001\u0000\u0000"+
		"\u0000\u0476\u0479\u0005\u0016\u0000\u0000\u0477\u0479\u0003(\u0014\u0000"+
		"\u0478\u0476\u0001\u0000\u0000\u0000\u0478\u0477\u0001\u0000\u0000\u0000"+
		"\u0479\'\u0001\u0000\u0000\u0000\u047a\u047b\u0007\u0007\u0000\u0000\u047b"+
		")\u0001\u0000\u0000\u0000\u047c\u047d\u0006\u0015\uffff\uffff\u0000\u047d"+
		"\u047e\u0003,\u0016\u0000\u047e\u048d\u0001\u0000\u0000\u0000\u047f\u0480"+
		"\n\u0002\u0000\u0000\u0480\u0482\u0005w\u0000\u0000\u0481\u0483\u0003"+
		">\u001f\u0000\u0482\u0481\u0001\u0000\u0000\u0000\u0482\u0483\u0001\u0000"+
		"\u0000\u0000\u0483\u0484\u0001\u0000\u0000\u0000\u0484\u048c\u0003*\u0015"+
		"\u0003\u0485\u0486\n\u0001\u0000\u0000\u0486\u0488\u0007\b\u0000\u0000"+
		"\u0487\u0489\u0003>\u001f\u0000\u0488\u0487\u0001\u0000\u0000\u0000\u0488"+
		"\u0489\u0001\u0000\u0000\u0000\u0489\u048a\u0001\u0000\u0000\u0000\u048a"+
		"\u048c\u0003*\u0015\u0002\u048b\u047f\u0001\u0000\u0000\u0000\u048b\u0485"+
		"\u0001\u0000\u0000\u0000\u048c\u048f\u0001\u0000\u0000\u0000\u048d\u048b"+
		"\u0001\u0000\u0000\u0000\u048d\u048e\u0001\u0000\u0000\u0000\u048e+\u0001"+
		"\u0000\u0000\u0000\u048f\u048d\u0001\u0000\u0000\u0000\u0490\u04a1\u0003"+
		"0\u0018\u0000\u0491\u0492\u0005\u00f0\u0000\u0000\u0492\u04a1\u0003\u00ce"+
		"g\u0000\u0493\u0494\u0005\u0113\u0000\u0000\u0494\u0499\u0003t:\u0000"+
		"\u0495\u0496\u0005\u0004\u0000\u0000\u0496\u0498\u0003t:\u0000\u0497\u0495"+
		"\u0001\u0000\u0000\u0000\u0498\u049b\u0001\u0000\u0000\u0000\u0499\u0497"+
		"\u0001\u0000\u0000\u0000\u0499\u049a\u0001\u0000\u0000\u0000\u049a\u04a1"+
		"\u0001\u0000\u0000\u0000\u049b\u0499\u0001\u0000\u0000\u0000\u049c\u049d"+
		"\u0005\u0002\u0000\u0000\u049d\u049e\u0003$\u0012\u0000\u049e\u049f\u0005"+
		"\u0003\u0000\u0000\u049f\u04a1\u0001\u0000\u0000\u0000\u04a0\u0490\u0001"+
		"\u0000\u0000\u0000\u04a0\u0491\u0001\u0000\u0000\u0000\u04a0\u0493\u0001"+
		"\u0000\u0000\u0000\u04a0\u049c\u0001\u0000\u0000\u0000\u04a1-\u0001\u0000"+
		"\u0000\u0000\u04a2\u04a4\u0003t:\u0000\u04a3\u04a5\u0007\t\u0000\u0000"+
		"\u04a4\u04a3\u0001\u0000\u0000\u0000\u04a4\u04a5\u0001\u0000\u0000\u0000"+
		"\u04a5\u04a8\u0001\u0000\u0000\u0000\u04a6\u04a7\u0005\u00aa\u0000\u0000"+
		"\u04a7\u04a9\u0007\n\u0000\u0000\u04a8\u04a6\u0001\u0000\u0000\u0000\u04a8"+
		"\u04a9\u0001\u0000\u0000\u0000\u04a9/\u0001\u0000\u0000\u0000\u04aa\u04ac"+
		"\u0005\u00e4\u0000\u0000\u04ab\u04ad\u0003>\u001f\u0000\u04ac\u04ab\u0001"+
		"\u0000\u0000\u0000\u04ac\u04ad\u0001\u0000\u0000\u0000\u04ad\u04ae\u0001"+
		"\u0000\u0000\u0000\u04ae\u04b3\u0003@ \u0000\u04af\u04b0\u0005\u0004\u0000"+
		"\u0000\u04b0\u04b2\u0003@ \u0000\u04b1\u04af\u0001\u0000\u0000\u0000\u04b2"+
		"\u04b5\u0001\u0000\u0000\u0000\u04b3\u04b1\u0001\u0000\u0000\u0000\u04b3"+
		"\u04b4\u0001\u0000\u0000\u0000\u04b4\u04bf\u0001\u0000\u0000\u0000\u04b5"+
		"\u04b3\u0001\u0000\u0000\u0000\u04b6\u04b7\u0005c\u0000\u0000\u04b7\u04bc"+
		"\u0003B!\u0000\u04b8\u04b9\u0005\u0004\u0000\u0000\u04b9\u04bb\u0003B"+
		"!\u0000\u04ba\u04b8\u0001\u0000\u0000\u0000\u04bb\u04be\u0001\u0000\u0000"+
		"\u0000\u04bc\u04ba\u0001\u0000\u0000\u0000\u04bc\u04bd\u0001\u0000\u0000"+
		"\u0000\u04bd\u04c0\u0001\u0000\u0000\u0000\u04be\u04bc\u0001\u0000\u0000"+
		"\u0000\u04bf\u04b6\u0001\u0000\u0000\u0000\u04bf\u04c0\u0001\u0000\u0000"+
		"\u0000\u04c0\u04c3\u0001\u0000\u0000\u0000\u04c1\u04c2\u0005\u0118\u0000"+
		"\u0000\u04c2\u04c4\u0003v;\u0000\u04c3\u04c1\u0001\u0000\u0000\u0000\u04c3"+
		"\u04c4\u0001\u0000\u0000\u0000\u04c4\u04c8\u0001\u0000\u0000\u0000\u04c5"+
		"\u04c6\u0005j\u0000\u0000\u04c6\u04c7\u0005#\u0000\u0000\u04c7\u04c9\u0003"+
		"2\u0019\u0000\u04c8\u04c5\u0001\u0000\u0000\u0000\u04c8\u04c9\u0001\u0000"+
		"\u0000\u0000\u04c9\u04cc\u0001\u0000\u0000\u0000\u04ca\u04cb\u0005m\u0000"+
		"\u0000\u04cb\u04cd\u0003v;\u0000\u04cc\u04ca\u0001\u0000\u0000\u0000\u04cc"+
		"\u04cd\u0001\u0000\u0000\u0000\u04cd\u04d7\u0001\u0000\u0000\u0000\u04ce"+
		"\u04cf\u0005\u0119\u0000\u0000\u04cf\u04d4\u00038\u001c\u0000\u04d0\u04d1"+
		"\u0005\u0004\u0000\u0000\u04d1\u04d3\u00038\u001c\u0000\u04d2\u04d0\u0001"+
		"\u0000\u0000\u0000\u04d3\u04d6\u0001\u0000\u0000\u0000\u04d4\u04d2\u0001"+
		"\u0000\u0000\u0000\u04d4\u04d5\u0001\u0000\u0000\u0000\u04d5\u04d8\u0001"+
		"\u0000\u0000\u0000\u04d6\u04d4\u0001\u0000\u0000\u0000\u04d7\u04ce\u0001"+
		"\u0000\u0000\u0000\u04d7\u04d8\u0001\u0000\u0000\u0000\u04d81\u0001\u0000"+
		"\u0000\u0000\u04d9\u04db\u0003>\u001f\u0000\u04da\u04d9\u0001\u0000\u0000"+
		"\u0000\u04da\u04db\u0001\u0000\u0000\u0000\u04db\u04dc\u0001\u0000\u0000"+
		"\u0000\u04dc\u04e1\u00034\u001a\u0000\u04dd\u04de\u0005\u0004\u0000\u0000"+
		"\u04de\u04e0\u00034\u001a\u0000\u04df\u04dd\u0001\u0000\u0000\u0000\u04e0"+
		"\u04e3\u0001\u0000\u0000\u0000\u04e1\u04df\u0001\u0000\u0000\u0000\u04e1"+
		"\u04e2\u0001\u0000\u0000\u0000\u04e23\u0001\u0000\u0000\u0000\u04e3\u04e1"+
		"\u0001\u0000\u0000\u0000\u04e4\u050d\u00036\u001b\u0000\u04e5\u04e6\u0005"+
		"\u00da\u0000\u0000\u04e6\u04ef\u0005\u0002\u0000\u0000\u04e7\u04ec\u0003"+
		"t:\u0000\u04e8\u04e9\u0005\u0004\u0000\u0000\u04e9\u04eb\u0003t:\u0000"+
		"\u04ea\u04e8\u0001\u0000\u0000\u0000\u04eb\u04ee\u0001\u0000\u0000\u0000"+
		"\u04ec\u04ea\u0001\u0000\u0000\u0000\u04ec\u04ed\u0001\u0000\u0000\u0000"+
		"\u04ed\u04f0\u0001\u0000\u0000\u0000\u04ee\u04ec\u0001\u0000\u0000\u0000"+
		"\u04ef\u04e7\u0001\u0000\u0000\u0000\u04ef\u04f0\u0001\u0000\u0000\u0000"+
		"\u04f0\u04f1\u0001\u0000\u0000\u0000\u04f1\u050d\u0005\u0003\u0000\u0000"+
		"\u04f2\u04f3\u00055\u0000\u0000\u04f3\u04fc\u0005\u0002\u0000\u0000\u04f4"+
		"\u04f9\u0003t:\u0000\u04f5\u04f6\u0005\u0004\u0000\u0000\u04f6\u04f8\u0003"+
		"t:\u0000\u04f7\u04f5\u0001\u0000\u0000\u0000\u04f8\u04fb\u0001\u0000\u0000"+
		"\u0000\u04f9\u04f7\u0001\u0000\u0000\u0000\u04f9\u04fa\u0001\u0000\u0000"+
		"\u0000\u04fa\u04fd\u0001\u0000\u0000\u0000\u04fb\u04f9\u0001\u0000\u0000"+
		"\u0000\u04fc\u04f4\u0001\u0000\u0000\u0000\u04fc\u04fd\u0001\u0000\u0000"+
		"\u0000\u04fd\u04fe\u0001\u0000\u0000\u0000\u04fe\u050d\u0005\u0003\u0000"+
		"\u0000\u04ff\u0500\u0005k\u0000\u0000\u0500\u0501\u0005\u00e8\u0000\u0000"+
		"\u0501\u0502\u0005\u0002\u0000\u0000\u0502\u0507\u00036\u001b\u0000\u0503"+
		"\u0504\u0005\u0004\u0000\u0000\u0504\u0506\u00036\u001b\u0000\u0505\u0503"+
		"\u0001\u0000\u0000\u0000\u0506\u0509\u0001\u0000\u0000\u0000\u0507\u0505"+
		"\u0001\u0000\u0000\u0000\u0507\u0508\u0001\u0000\u0000\u0000\u0508\u050a"+
		"\u0001\u0000\u0000\u0000\u0509\u0507\u0001\u0000\u0000\u0000\u050a\u050b"+
		"\u0005\u0003\u0000\u0000\u050b\u050d\u0001\u0000\u0000\u0000\u050c\u04e4"+
		"\u0001\u0000\u0000\u0000\u050c\u04e5\u0001\u0000\u0000\u0000\u050c\u04f2"+
		"\u0001\u0000\u0000\u0000\u050c\u04ff\u0001\u0000\u0000\u0000\u050d5\u0001"+
		"\u0000\u0000\u0000\u050e\u0517\u0005\u0002\u0000\u0000\u050f\u0514\u0003"+
		"t:\u0000\u0510\u0511\u0005\u0004\u0000\u0000\u0511\u0513\u0003t:\u0000"+
		"\u0512\u0510\u0001\u0000\u0000\u0000\u0513\u0516\u0001\u0000\u0000\u0000"+
		"\u0514\u0512\u0001\u0000\u0000\u0000\u0514\u0515\u0001\u0000\u0000\u0000"+
		"\u0515\u0518\u0001\u0000\u0000\u0000\u0516\u0514\u0001\u0000\u0000\u0000"+
		"\u0517\u050f\u0001\u0000\u0000\u0000\u0517\u0518\u0001\u0000\u0000\u0000"+
		"\u0518\u0519\u0001\u0000\u0000\u0000\u0519\u051c\u0005\u0003\u0000\u0000"+
		"\u051a\u051c\u0003t:\u0000\u051b\u050e\u0001\u0000\u0000\u0000\u051b\u051a"+
		"\u0001\u0000\u0000\u0000\u051c7\u0001\u0000\u0000\u0000\u051d\u051e\u0003"+
		"\u00dam\u0000\u051e\u051f\u0005\u001c\u0000\u0000\u051f\u0520\u0005\u0002"+
		"\u0000\u0000\u0520\u0521\u0003:\u001d\u0000\u0521\u0522\u0005\u0003\u0000"+
		"\u0000\u05229\u0001\u0000\u0000\u0000\u0523\u0525\u0003\u00dam\u0000\u0524"+
		"\u0523\u0001\u0000\u0000\u0000\u0524\u0525\u0001\u0000\u0000\u0000\u0525"+
		"\u0530\u0001\u0000\u0000\u0000\u0526\u0527\u0005\u00ba\u0000\u0000\u0527"+
		"\u0528\u0005#\u0000\u0000\u0528\u052d\u0003t:\u0000\u0529\u052a\u0005"+
		"\u0004\u0000\u0000\u052a\u052c\u0003t:\u0000\u052b\u0529\u0001\u0000\u0000"+
		"\u0000\u052c\u052f\u0001\u0000\u0000\u0000\u052d\u052b\u0001\u0000\u0000"+
		"\u0000\u052d\u052e\u0001\u0000\u0000\u0000\u052e\u0531\u0001\u0000\u0000"+
		"\u0000\u052f\u052d\u0001\u0000\u0000\u0000\u0530\u0526\u0001\u0000\u0000"+
		"\u0000\u0530\u0531\u0001\u0000\u0000\u0000\u0531\u053c\u0001\u0000\u0000"+
		"\u0000\u0532\u0533\u0005\u00b4\u0000\u0000\u0533\u0534\u0005#\u0000\u0000"+
		"\u0534\u0539\u0003.\u0017\u0000\u0535\u0536\u0005\u0004\u0000\u0000\u0536"+
		"\u0538\u0003.\u0017\u0000\u0537\u0535\u0001\u0000\u0000\u0000\u0538\u053b"+
		"\u0001\u0000\u0000\u0000\u0539\u0537\u0001\u0000\u0000\u0000\u0539\u053a"+
		"\u0001\u0000\u0000\u0000\u053a\u053d\u0001\u0000\u0000\u0000\u053b\u0539"+
		"\u0001\u0000\u0000\u0000\u053c\u0532\u0001\u0000\u0000\u0000\u053c\u053d"+
		"\u0001\u0000\u0000\u0000\u053d\u053f\u0001\u0000\u0000\u0000\u053e\u0540"+
		"\u0003\u00b2Y\u0000\u053f\u053e\u0001\u0000\u0000\u0000\u053f\u0540\u0001"+
		"\u0000\u0000\u0000\u0540;\u0001\u0000\u0000\u0000\u0541\u0543\u0003\u00da"+
		"m\u0000\u0542\u0544\u0003b1\u0000\u0543\u0542\u0001\u0000\u0000\u0000"+
		"\u0543\u0544\u0001\u0000\u0000\u0000\u0544\u0545\u0001\u0000\u0000\u0000"+
		"\u0545\u0546\u0005\u001c\u0000\u0000\u0546\u0547\u0005\u0002\u0000\u0000"+
		"\u0547\u0548\u0003\f\u0006\u0000\u0548\u0549\u0005\u0003\u0000\u0000\u0549"+
		"=\u0001\u0000\u0000\u0000\u054a\u054b\u0007\u000b\u0000\u0000\u054b?\u0001"+
		"\u0000\u0000\u0000\u054c\u0551\u0003t:\u0000\u054d\u054f\u0005\u001c\u0000"+
		"\u0000\u054e\u054d\u0001\u0000\u0000\u0000\u054e\u054f\u0001\u0000\u0000"+
		"\u0000\u054f\u0550\u0001\u0000\u0000\u0000\u0550\u0552\u0003\u00dam\u0000"+
		"\u0551\u054e\u0001\u0000\u0000\u0000\u0551\u0552\u0001\u0000\u0000\u0000"+
		"\u0552\u055c\u0001\u0000\u0000\u0000\u0553\u0554\u0003|>\u0000\u0554\u0555"+
		"\u0005\u0001\u0000\u0000\u0555\u0558\u0005\u012a\u0000\u0000\u0556\u0557"+
		"\u0005\u001c\u0000\u0000\u0557\u0559\u0003b1\u0000\u0558\u0556\u0001\u0000"+
		"\u0000\u0000\u0558\u0559\u0001\u0000\u0000\u0000\u0559\u055c\u0001\u0000"+
		"\u0000\u0000\u055a\u055c\u0005\u012a\u0000\u0000\u055b\u054c\u0001\u0000"+
		"\u0000\u0000\u055b\u0553\u0001\u0000\u0000\u0000\u055b\u055a\u0001\u0000"+
		"\u0000\u0000\u055cA\u0001\u0000\u0000\u0000\u055d\u055e\u0006!\uffff\uffff"+
		"\u0000\u055e\u055f\u0003H$\u0000\u055f\u0572\u0001\u0000\u0000\u0000\u0560"+
		"\u056e\n\u0002\u0000\u0000\u0561\u0562\u00054\u0000\u0000\u0562\u0563"+
		"\u0005~\u0000\u0000\u0563\u056f\u0003H$\u0000\u0564\u0565\u0003D\"\u0000"+
		"\u0565\u0566\u0005~\u0000\u0000\u0566\u0567\u0003B!\u0000\u0567\u0568"+
		"\u0003F#\u0000\u0568\u056f\u0001\u0000\u0000\u0000\u0569\u056a\u0005\u009e"+
		"\u0000\u0000\u056a\u056b\u0003D\"\u0000\u056b\u056c\u0005~\u0000\u0000"+
		"\u056c\u056d\u0003H$\u0000\u056d\u056f\u0001\u0000\u0000\u0000\u056e\u0561"+
		"\u0001\u0000\u0000\u0000\u056e\u0564\u0001\u0000\u0000\u0000\u056e\u0569"+
		"\u0001\u0000\u0000\u0000\u056f\u0571\u0001\u0000\u0000\u0000\u0570\u0560"+
		"\u0001\u0000\u0000\u0000\u0571\u0574\u0001\u0000\u0000\u0000\u0572\u0570"+
		"\u0001\u0000\u0000\u0000\u0572\u0573\u0001\u0000\u0000\u0000\u0573C\u0001"+
		"\u0000\u0000\u0000\u0574\u0572\u0001\u0000\u0000\u0000\u0575\u0577\u0005"+
		"t\u0000\u0000\u0576\u0575\u0001\u0000\u0000\u0000\u0576\u0577\u0001\u0000"+
		"\u0000\u0000\u0577\u0585\u0001\u0000\u0000\u0000\u0578\u057a\u0005\u008b"+
		"\u0000\u0000\u0579\u057b\u0005\u00b6\u0000\u0000\u057a\u0579\u0001\u0000"+
		"\u0000\u0000\u057a\u057b\u0001\u0000\u0000\u0000\u057b\u0585\u0001\u0000"+
		"\u0000\u0000\u057c\u057e\u0005\u00d6\u0000\u0000\u057d\u057f\u0005\u00b6"+
		"\u0000\u0000\u057e\u057d\u0001\u0000\u0000\u0000\u057e\u057f\u0001\u0000"+
		"\u0000\u0000\u057f\u0585\u0001\u0000\u0000\u0000\u0580\u0582\u0005d\u0000"+
		"\u0000\u0581\u0583\u0005\u00b6\u0000\u0000\u0582\u0581\u0001\u0000\u0000"+
		"\u0000\u0582\u0583\u0001\u0000\u0000\u0000\u0583\u0585\u0001\u0000\u0000"+
		"\u0000\u0584\u0576\u0001\u0000\u0000\u0000\u0584\u0578\u0001\u0000\u0000"+
		"\u0000\u0584\u057c\u0001\u0000\u0000\u0000\u0584\u0580\u0001\u0000\u0000"+
		"\u0000\u0585E\u0001\u0000\u0000\u0000\u0586\u0587\u0005\u00af\u0000\u0000"+
		"\u0587\u0595\u0003v;\u0000\u0588\u0589\u0005\u010d\u0000\u0000\u0589\u058a"+
		"\u0005\u0002\u0000\u0000\u058a\u058f\u0003\u00dam\u0000\u058b\u058c\u0005"+
		"\u0004\u0000\u0000\u058c\u058e\u0003\u00dam\u0000\u058d\u058b\u0001\u0000"+
		"\u0000\u0000\u058e\u0591\u0001\u0000\u0000\u0000\u058f\u058d\u0001\u0000"+
		"\u0000\u0000\u058f\u0590\u0001\u0000\u0000\u0000\u0590\u0592\u0001\u0000"+
		"\u0000\u0000\u0591\u058f\u0001\u0000\u0000\u0000\u0592\u0593\u0005\u0003"+
		"\u0000\u0000\u0593\u0595\u0001\u0000\u0000\u0000\u0594\u0586\u0001\u0000"+
		"\u0000\u0000\u0594\u0588\u0001\u0000\u0000\u0000\u0595G\u0001\u0000\u0000"+
		"\u0000\u0596\u059d\u0003R)\u0000\u0597\u0598\u0005\u00f2\u0000\u0000\u0598"+
		"\u0599\u0003J%\u0000\u0599\u059a\u0005\u0002\u0000\u0000\u059a\u059b\u0003"+
		"t:\u0000\u059b\u059c\u0005\u0003\u0000\u0000\u059c\u059e\u0001\u0000\u0000"+
		"\u0000\u059d\u0597\u0001\u0000\u0000\u0000\u059d\u059e\u0001\u0000\u0000"+
		"\u0000\u059eI\u0001\u0000\u0000\u0000\u059f\u05a0\u0007\f\u0000\u0000"+
		"\u05a0K\u0001\u0000\u0000\u0000\u05a1\u05a2\u0007\r\u0000\u0000\u05a2"+
		"M\u0001\u0000\u0000\u0000\u05a3\u05aa\u0005S\u0000\u0000\u05a4\u05a6\u0005"+
		"\u00fe\u0000\u0000\u05a5\u05a7\u0003\u0094J\u0000\u05a6\u05a5\u0001\u0000"+
		"\u0000\u0000\u05a6\u05a7\u0001\u0000\u0000\u0000\u05a7\u05a8\u0001\u0000"+
		"\u0000\u0000\u05a8\u05aa\u0003P(\u0000\u05a9\u05a3\u0001\u0000\u0000\u0000"+
		"\u05a9\u05a4\u0001\u0000\u0000\u0000\u05aaO\u0001\u0000\u0000\u0000\u05ab"+
		"\u05ac\u0005\u011a\u0000\u0000\u05ac\u05b0\u00051\u0000\u0000\u05ad\u05ae"+
		"\u0005\u011c\u0000\u0000\u05ae\u05b0\u00051\u0000\u0000\u05af\u05ab\u0001"+
		"\u0000\u0000\u0000\u05af\u05ad\u0001\u0000\u0000\u0000\u05b0Q\u0001\u0000"+
		"\u0000\u0000\u05b1\u0604\u0003`0\u0000\u05b2\u05b3\u0005\u0098\u0000\u0000"+
		"\u05b3\u05be\u0005\u0002\u0000\u0000\u05b4\u05b5\u0005\u00ba\u0000\u0000"+
		"\u05b5\u05b6\u0005#\u0000\u0000\u05b6\u05bb\u0003t:\u0000\u05b7\u05b8"+
		"\u0005\u0004\u0000\u0000\u05b8\u05ba\u0003t:\u0000\u05b9\u05b7\u0001\u0000"+
		"\u0000\u0000\u05ba\u05bd\u0001\u0000\u0000\u0000\u05bb\u05b9\u0001\u0000"+
		"\u0000\u0000\u05bb\u05bc\u0001\u0000\u0000\u0000\u05bc\u05bf\u0001\u0000"+
		"\u0000\u0000\u05bd\u05bb\u0001\u0000\u0000\u0000\u05be\u05b4\u0001\u0000"+
		"\u0000\u0000\u05be\u05bf\u0001\u0000\u0000\u0000\u05bf\u05ca\u0001\u0000"+
		"\u0000\u0000\u05c0\u05c1\u0005\u00b4\u0000\u0000\u05c1\u05c2\u0005#\u0000"+
		"\u0000\u05c2\u05c7\u0003.\u0017\u0000\u05c3\u05c4\u0005\u0004\u0000\u0000"+
		"\u05c4\u05c6\u0003.\u0017\u0000\u05c5\u05c3\u0001\u0000\u0000\u0000\u05c6"+
		"\u05c9\u0001\u0000\u0000\u0000\u05c7\u05c5\u0001\u0000\u0000\u0000\u05c7"+
		"\u05c8\u0001\u0000\u0000\u0000\u05c8\u05cb\u0001\u0000\u0000\u0000\u05c9"+
		"\u05c7\u0001\u0000\u0000\u0000\u05ca\u05c0\u0001\u0000\u0000\u0000\u05ca"+
		"\u05cb\u0001\u0000\u0000\u0000\u05cb\u05d5\u0001\u0000\u0000\u0000\u05cc"+
		"\u05cd\u0005\u009a\u0000\u0000\u05cd\u05d2\u0003T*\u0000\u05ce\u05cf\u0005"+
		"\u0004\u0000\u0000\u05cf\u05d1\u0003T*\u0000\u05d0\u05ce\u0001\u0000\u0000"+
		"\u0000\u05d1\u05d4\u0001\u0000\u0000\u0000\u05d2\u05d0\u0001\u0000\u0000"+
		"\u0000\u05d2\u05d3\u0001\u0000\u0000\u0000\u05d3\u05d6\u0001\u0000\u0000"+
		"\u0000\u05d4\u05d2\u0001\u0000\u0000\u0000\u05d5\u05cc\u0001\u0000\u0000"+
		"\u0000\u05d5\u05d6\u0001\u0000\u0000\u0000\u05d6\u05d8\u0001\u0000\u0000"+
		"\u0000\u05d7\u05d9\u0003V+\u0000\u05d8\u05d7\u0001\u0000\u0000\u0000\u05d8"+
		"\u05d9\u0001\u0000\u0000\u0000\u05d9\u05dd\u0001\u0000\u0000\u0000\u05da"+
		"\u05db\u0005\u0015\u0000\u0000\u05db\u05dc\u0005\u0095\u0000\u0000\u05dc"+
		"\u05de\u0003Z-\u0000\u05dd\u05da\u0001\u0000\u0000\u0000\u05dd\u05de\u0001"+
		"\u0000\u0000\u0000\u05de\u05e0\u0001\u0000\u0000\u0000\u05df\u05e1\u0007"+
		"\u000e\u0000\u0000\u05e0\u05df\u0001\u0000\u0000\u0000\u05e0\u05e1\u0001"+
		"\u0000\u0000\u0000\u05e1\u05e2\u0001\u0000\u0000\u0000\u05e2\u05e3\u0005"+
		"\u00bf\u0000\u0000\u05e3\u05e4\u0005\u0002\u0000\u0000\u05e4\u05e5\u0003"+
		"\u00b8\\\u0000\u05e5\u05ef\u0005\u0003\u0000\u0000\u05e6\u05e7\u0005\u00ed"+
		"\u0000\u0000\u05e7\u05ec\u0003\\.\u0000\u05e8\u05e9\u0005\u0004\u0000"+
		"\u0000\u05e9\u05eb\u0003\\.\u0000\u05ea\u05e8\u0001\u0000\u0000\u0000"+
		"\u05eb\u05ee\u0001\u0000\u0000\u0000\u05ec\u05ea\u0001\u0000\u0000\u0000"+
		"\u05ec\u05ed\u0001\u0000\u0000\u0000\u05ed\u05f0\u0001\u0000\u0000\u0000"+
		"\u05ee\u05ec\u0001\u0000\u0000\u0000\u05ef\u05e6\u0001\u0000\u0000\u0000"+
		"\u05ef\u05f0\u0001\u0000\u0000\u0000\u05f0\u05f1\u0001\u0000\u0000\u0000"+
		"\u05f1\u05f2\u0005D\u0000\u0000\u05f2\u05f7\u0003^/\u0000\u05f3\u05f4"+
		"\u0005\u0004\u0000\u0000\u05f4\u05f6\u0003^/\u0000\u05f5\u05f3\u0001\u0000"+
		"\u0000\u0000\u05f6\u05f9\u0001\u0000\u0000\u0000\u05f7\u05f5\u0001\u0000"+
		"\u0000\u0000\u05f7\u05f8\u0001\u0000\u0000\u0000\u05f8\u05fa\u0001\u0000"+
		"\u0000\u0000\u05f9\u05f7\u0001\u0000\u0000\u0000\u05fa\u0602\u0005\u0003"+
		"\u0000\u0000\u05fb\u05fd\u0005\u001c\u0000\u0000\u05fc\u05fb\u0001\u0000"+
		"\u0000\u0000\u05fc\u05fd\u0001\u0000\u0000\u0000\u05fd\u05fe\u0001\u0000"+
		"\u0000\u0000\u05fe\u0600\u0003\u00dam\u0000\u05ff\u0601\u0003b1\u0000"+
		"\u0600\u05ff\u0001\u0000\u0000\u0000\u0600\u0601\u0001\u0000\u0000\u0000"+
		"\u0601\u0603\u0001\u0000\u0000\u0000\u0602\u05fc\u0001\u0000\u0000\u0000"+
		"\u0602\u0603\u0001\u0000\u0000\u0000\u0603\u0605\u0001\u0000\u0000\u0000"+
		"\u0604\u05b2\u0001\u0000\u0000\u0000\u0604\u0605\u0001\u0000\u0000\u0000"+
		"\u0605S\u0001\u0000\u0000\u0000\u0606\u0607\u0003t:\u0000\u0607\u0608"+
		"\u0005\u001c\u0000\u0000\u0608\u0609\u0003\u00dam\u0000\u0609U\u0001\u0000"+
		"\u0000\u0000\u060a\u060b\u0005\u00b0\u0000\u0000\u060b\u060c\u0005\u00db"+
		"\u0000\u0000\u060c\u060d\u0005\u00c0\u0000\u0000\u060d\u0616\u0005\u0095"+
		"\u0000\u0000\u060e\u060f\u0005\u0016\u0000\u0000\u060f\u0610\u0005\u00dc"+
		"\u0000\u0000\u0610\u0611\u0005\u00c0\u0000\u0000\u0611\u0613\u0005\u0095"+
		"\u0000\u0000\u0612\u0614\u0003X,\u0000\u0613\u0612\u0001\u0000\u0000\u0000"+
		"\u0613\u0614\u0001\u0000\u0000\u0000\u0614\u0616\u0001\u0000\u0000\u0000"+
		"\u0615\u060a\u0001\u0000\u0000\u0000\u0615\u060e\u0001\u0000\u0000\u0000"+
		"\u0616W\u0001\u0000\u0000\u0000\u0617\u0618\u0005\u00e9\u0000\u0000\u0618"+
		"\u0619\u0005P\u0000\u0000\u0619\u0621\u0005\u0097\u0000\u0000\u061a\u061b"+
		"\u0005\u00ae\u0000\u0000\u061b\u061c\u0005P\u0000\u0000\u061c\u0621\u0005"+
		"\u0097\u0000\u0000\u061d\u061e\u0005\u011a\u0000\u0000\u061e\u061f\u0005"+
		"\u0108\u0000\u0000\u061f\u0621\u0005\u00dc\u0000\u0000\u0620\u0617\u0001"+
		"\u0000\u0000\u0000\u0620\u061a\u0001\u0000\u0000\u0000\u0620\u061d\u0001"+
		"\u0000\u0000\u0000\u0621Y\u0001\u0000\u0000\u0000\u0622\u0623\u0005\u0005"+
		"\u0000\u0000\u0623\u0624\u0005\u00f9\u0000\u0000\u0624\u0625\u0005\u009f"+
		"\u0000\u0000\u0625\u0636\u0005\u00db\u0000\u0000\u0626\u0627\u0005\u0005"+
		"\u0000\u0000\u0627\u0628\u0005\u00bd\u0000\u0000\u0628\u0629\u0005\u0088"+
		"\u0000\u0000\u0629\u0636\u0005\u00db\u0000\u0000\u062a\u062b\u0005\u0005"+
		"\u0000\u0000\u062b\u062c\u0005\u00f9\u0000\u0000\u062c\u062d\u0005_\u0000"+
		"\u0000\u062d\u0636\u0003\u00dam\u0000\u062e\u062f\u0005\u0005\u0000\u0000"+
		"\u062f\u0630\u0005\u00f9\u0000\u0000\u0630\u0631\u0005\u0088\u0000\u0000"+
		"\u0631\u0636\u0003\u00dam\u0000\u0632\u0633\u0005\u0005\u0000\u0000\u0633"+
		"\u0634\u0005\u00f9\u0000\u0000\u0634\u0636\u0003\u00dam\u0000\u0635\u0622"+
		"\u0001\u0000\u0000\u0000\u0635\u0626\u0001\u0000\u0000\u0000\u0635\u062a"+
		"\u0001\u0000\u0000\u0000\u0635\u062e\u0001\u0000\u0000\u0000\u0635\u0632"+
		"\u0001\u0000\u0000\u0000\u0636[\u0001\u0000\u0000\u0000\u0637\u0638\u0003"+
		"\u00dam\u0000\u0638\u0639\u0005\u0122\u0000\u0000\u0639\u063a\u0005\u0002"+
		"\u0000\u0000\u063a\u063f\u0003\u00dam\u0000\u063b\u063c\u0005\u0004\u0000"+
		"\u0000\u063c\u063e\u0003\u00dam\u0000\u063d\u063b\u0001\u0000\u0000\u0000"+
		"\u063e\u0641\u0001\u0000\u0000\u0000\u063f\u063d\u0001\u0000\u0000\u0000"+
		"\u063f\u0640\u0001\u0000\u0000\u0000\u0640\u0642\u0001\u0000\u0000\u0000"+
		"\u0641\u063f\u0001\u0000\u0000\u0000\u0642\u0643\u0005\u0003\u0000\u0000"+
		"\u0643]\u0001\u0000\u0000\u0000\u0644\u0645\u0003\u00dam\u0000\u0645\u0646"+
		"\u0005\u001c\u0000\u0000\u0646\u0647\u0003t:\u0000\u0647_\u0001\u0000"+
		"\u0000\u0000\u0648\u0650\u0003d2\u0000\u0649\u064b\u0005\u001c\u0000\u0000"+
		"\u064a\u0649\u0001\u0000\u0000\u0000\u064a\u064b\u0001\u0000\u0000\u0000"+
		"\u064b\u064c\u0001\u0000\u0000\u0000\u064c\u064e\u0003\u00dam\u0000\u064d"+
		"\u064f\u0003b1\u0000\u064e\u064d\u0001\u0000\u0000\u0000\u064e\u064f\u0001"+
		"\u0000\u0000\u0000\u064f\u0651\u0001\u0000\u0000\u0000\u0650\u064a\u0001"+
		"\u0000\u0000\u0000\u0650\u0651\u0001\u0000\u0000\u0000\u0651a\u0001\u0000"+
		"\u0000\u0000\u0652\u0653\u0005\u0002\u0000\u0000\u0653\u0658\u0003\u00da"+
		"m\u0000\u0654\u0655\u0005\u0004\u0000\u0000\u0655\u0657\u0003\u00dam\u0000"+
		"\u0656\u0654\u0001\u0000\u0000\u0000\u0657\u065a\u0001\u0000\u0000\u0000"+
		"\u0658\u0656\u0001\u0000\u0000\u0000\u0658\u0659\u0001\u0000\u0000\u0000"+
		"\u0659\u065b\u0001\u0000\u0000\u0000\u065a\u0658\u0001\u0000\u0000\u0000"+
		"\u065b\u065c\u0005\u0003\u0000\u0000\u065cc\u0001\u0000\u0000\u0000\u065d"+
		"\u065f\u0003\u00ceg\u0000\u065e\u0660\u0003\u00d0h\u0000\u065f\u065e\u0001"+
		"\u0000\u0000\u0000\u065f\u0660\u0001\u0000\u0000\u0000\u0660\u0683\u0001"+
		"\u0000\u0000\u0000\u0661\u0662\u0005\u0002\u0000\u0000\u0662\u0663\u0003"+
		"\f\u0006\u0000\u0663\u0664\u0005\u0003\u0000\u0000\u0664\u0683\u0001\u0000"+
		"\u0000\u0000\u0665\u0666\u0005\u0109\u0000\u0000\u0666\u0667\u0005\u0002"+
		"\u0000\u0000\u0667\u066c\u0003t:\u0000\u0668\u0669\u0005\u0004\u0000\u0000"+
		"\u0669\u066b\u0003t:\u0000\u066a\u0668\u0001\u0000\u0000\u0000\u066b\u066e"+
		"\u0001\u0000\u0000\u0000\u066c\u066a\u0001\u0000\u0000\u0000\u066c\u066d"+
		"\u0001\u0000\u0000\u0000\u066d\u066f\u0001\u0000\u0000\u0000\u066e\u066c"+
		"\u0001\u0000\u0000\u0000\u066f\u0672\u0005\u0003\u0000\u0000\u0670\u0671"+
		"\u0005\u011a\u0000\u0000\u0671\u0673\u0005\u00b5\u0000\u0000\u0672\u0670"+
		"\u0001\u0000\u0000\u0000\u0672\u0673\u0001\u0000\u0000\u0000\u0673\u0683"+
		"\u0001\u0000\u0000\u0000\u0674\u0675\u0005\u0089\u0000\u0000\u0675\u0676"+
		"\u0005\u0002\u0000\u0000\u0676\u0677\u0003\f\u0006\u0000\u0677\u0678\u0005"+
		"\u0003\u0000\u0000\u0678\u0683\u0001\u0000\u0000\u0000\u0679\u067a\u0005"+
		"\u00f0\u0000\u0000\u067a\u067b\u0005\u0002\u0000\u0000\u067b\u067c\u0003"+
		"f3\u0000\u067c\u067d\u0005\u0003\u0000\u0000\u067d\u0683\u0001\u0000\u0000"+
		"\u0000\u067e\u067f\u0005\u0002\u0000\u0000\u067f\u0680\u0003B!\u0000\u0680"+
		"\u0681\u0005\u0003\u0000\u0000\u0681\u0683\u0001\u0000\u0000\u0000\u0682"+
		"\u065d\u0001\u0000\u0000\u0000\u0682\u0661\u0001\u0000\u0000\u0000\u0682"+
		"\u0665\u0001\u0000\u0000\u0000\u0682\u0674\u0001\u0000\u0000\u0000\u0682"+
		"\u0679\u0001\u0000\u0000\u0000\u0682\u067e\u0001\u0000\u0000\u0000\u0683"+
		"e\u0001\u0000\u0000\u0000\u0684\u0685\u0003\u00ceg\u0000\u0685\u068e\u0005"+
		"\u0002\u0000\u0000\u0686\u068b\u0003h4\u0000\u0687\u0688\u0005\u0004\u0000"+
		"\u0000\u0688\u068a\u0003h4\u0000\u0689\u0687\u0001\u0000\u0000\u0000\u068a"+
		"\u068d\u0001\u0000\u0000\u0000\u068b\u0689\u0001\u0000\u0000\u0000\u068b"+
		"\u068c\u0001\u0000\u0000\u0000\u068c\u068f\u0001\u0000\u0000\u0000\u068d"+
		"\u068b\u0001\u0000\u0000\u0000\u068e\u0686\u0001\u0000\u0000\u0000\u068e"+
		"\u068f\u0001\u0000\u0000\u0000\u068f\u0699\u0001\u0000\u0000\u0000\u0690"+
		"\u0691\u00052\u0000\u0000\u0691\u0696\u0003r9\u0000\u0692\u0693\u0005"+
		"\u0004\u0000\u0000\u0693\u0695\u0003r9\u0000\u0694\u0692\u0001\u0000\u0000"+
		"\u0000\u0695\u0698\u0001\u0000\u0000\u0000\u0696\u0694\u0001\u0000\u0000"+
		"\u0000\u0696\u0697\u0001\u0000\u0000\u0000\u0697\u069a\u0001\u0000\u0000"+
		"\u0000\u0698\u0696\u0001\u0000\u0000\u0000\u0699\u0690\u0001\u0000\u0000"+
		"\u0000\u0699\u069a\u0001\u0000\u0000\u0000\u069a\u069b\u0001\u0000\u0000"+
		"\u0000\u069b\u069c\u0005\u0003\u0000\u0000\u069cg\u0001\u0000\u0000\u0000"+
		"\u069d\u069e\u0003\u00dam\u0000\u069e\u069f\u0005\u0006\u0000\u0000\u069f"+
		"\u06a1\u0001\u0000\u0000\u0000\u06a0\u069d\u0001\u0000\u0000\u0000\u06a0"+
		"\u06a1\u0001\u0000\u0000\u0000\u06a1\u06a5\u0001\u0000\u0000\u0000\u06a2"+
		"\u06a6\u0003j5\u0000\u06a3\u06a6\u0003n7\u0000\u06a4\u06a6\u0003t:\u0000"+
		"\u06a5\u06a2\u0001\u0000\u0000\u0000\u06a5\u06a3\u0001\u0000\u0000\u0000"+
		"\u06a5\u06a4\u0001\u0000\u0000\u0000\u06a6i\u0001\u0000\u0000\u0000\u06a7"+
		"\u06b9\u0003l6\u0000\u06a8\u06a9\u0005\u00ba\u0000\u0000\u06a9\u06b7\u0005"+
		"#\u0000\u0000\u06aa\u06b3\u0005\u0002\u0000\u0000\u06ab\u06b0\u0003t:"+
		"\u0000\u06ac\u06ad\u0005\u0004\u0000\u0000\u06ad\u06af\u0003t:\u0000\u06ae"+
		"\u06ac\u0001\u0000\u0000\u0000\u06af\u06b2\u0001\u0000\u0000\u0000\u06b0"+
		"\u06ae\u0001\u0000\u0000\u0000\u06b0\u06b1\u0001\u0000\u0000\u0000\u06b1"+
		"\u06b4\u0001\u0000\u0000\u0000\u06b2\u06b0\u0001\u0000\u0000\u0000\u06b3"+
		"\u06ab\u0001\u0000\u0000\u0000\u06b3\u06b4\u0001\u0000\u0000\u0000\u06b4"+
		"\u06b5\u0001\u0000\u0000\u0000\u06b5\u06b8\u0005\u0003\u0000\u0000\u06b6"+
		"\u06b8\u0003t:\u0000\u06b7\u06aa\u0001\u0000\u0000\u0000\u06b7\u06b6\u0001"+
		"\u0000\u0000\u0000\u06b8\u06ba\u0001\u0000\u0000\u0000\u06b9\u06a8\u0001"+
		"\u0000\u0000\u0000\u06b9\u06ba\u0001\u0000\u0000\u0000\u06ba\u06c1\u0001"+
		"\u0000\u0000\u0000\u06bb\u06bc\u0005\u00c8\u0000\u0000\u06bc\u06bd\u0005"+
		"\u0117\u0000\u0000\u06bd\u06c2\u0005P\u0000\u0000\u06be\u06bf\u0005\u0085"+
		"\u0000\u0000\u06bf\u06c0\u0005\u0117\u0000\u0000\u06c0\u06c2\u0005P\u0000"+
		"\u0000\u06c1\u06bb\u0001\u0000\u0000\u0000\u06c1\u06be\u0001\u0000\u0000"+
		"\u0000\u06c1\u06c2\u0001\u0000\u0000\u0000\u06c2\u06d3\u0001\u0000\u0000"+
		"\u0000\u06c3\u06c4\u0005\u00b4\u0000\u0000\u06c4\u06d1\u0005#\u0000\u0000"+
		"\u06c5\u06c6\u0005\u0002\u0000\u0000\u06c6\u06cb\u0003.\u0017\u0000\u06c7"+
		"\u06c8\u0005\u0004\u0000\u0000\u06c8\u06ca\u0003.\u0017\u0000\u06c9\u06c7"+
		"\u0001\u0000\u0000\u0000\u06ca\u06cd\u0001\u0000\u0000\u0000\u06cb\u06c9"+
		"\u0001\u0000\u0000\u0000\u06cb\u06cc\u0001\u0000\u0000\u0000\u06cc\u06ce"+
		"\u0001\u0000\u0000\u0000\u06cd\u06cb\u0001\u0000\u0000\u0000\u06ce\u06cf"+
		"\u0005\u0003\u0000\u0000\u06cf\u06d2\u0001\u0000\u0000\u0000\u06d0\u06d2"+
		"\u0003.\u0017\u0000\u06d1\u06c5\u0001\u0000\u0000\u0000\u06d1\u06d0\u0001"+
		"\u0000\u0000\u0000\u06d2\u06d4\u0001\u0000\u0000\u0000\u06d3\u06c3\u0001"+
		"\u0000\u0000\u0000\u06d3\u06d4\u0001\u0000\u0000\u0000\u06d4k\u0001\u0000"+
		"\u0000\u0000\u06d5\u06d6\u0005\u00f0\u0000\u0000\u06d6\u06d7\u0005\u0002"+
		"\u0000\u0000\u06d7\u06d8\u0003\u00ceg\u0000\u06d8\u06e0\u0005\u0003\u0000"+
		"\u0000\u06d9\u06db\u0005\u001c\u0000\u0000\u06da\u06d9\u0001\u0000\u0000"+
		"\u0000\u06da\u06db\u0001\u0000\u0000\u0000\u06db\u06dc\u0001\u0000\u0000"+
		"\u0000\u06dc\u06de\u0003\u00dam\u0000\u06dd\u06df\u0003b1\u0000\u06de"+
		"\u06dd\u0001\u0000\u0000\u0000\u06de\u06df\u0001\u0000\u0000\u0000\u06df"+
		"\u06e1\u0001\u0000\u0000\u0000\u06e0\u06da\u0001\u0000\u0000\u0000\u06e0"+
		"\u06e1\u0001\u0000\u0000\u0000\u06e1\u06f0\u0001\u0000\u0000\u0000\u06e2"+
		"\u06e3\u0005\u00f0\u0000\u0000\u06e3\u06e4\u0005\u0002\u0000\u0000\u06e4"+
		"\u06e5\u0003\f\u0006\u0000\u06e5\u06ed\u0005\u0003\u0000\u0000\u06e6\u06e8"+
		"\u0005\u001c\u0000\u0000\u06e7\u06e6\u0001\u0000\u0000\u0000\u06e7\u06e8"+
		"\u0001\u0000\u0000\u0000\u06e8\u06e9\u0001\u0000\u0000\u0000\u06e9\u06eb"+
		"\u0003\u00dam\u0000\u06ea\u06ec\u0003b1\u0000\u06eb\u06ea\u0001\u0000"+
		"\u0000\u0000\u06eb\u06ec\u0001\u0000\u0000\u0000\u06ec\u06ee\u0001\u0000"+
		"\u0000\u0000\u06ed\u06e7\u0001\u0000\u0000\u0000\u06ed\u06ee\u0001\u0000"+
		"\u0000\u0000\u06ee\u06f0\u0001\u0000\u0000\u0000\u06ef\u06d5\u0001\u0000"+
		"\u0000\u0000\u06ef\u06e2\u0001\u0000\u0000\u0000\u06f0m\u0001\u0000\u0000"+
		"\u0000\u06f1\u06f2\u0005J\u0000\u0000\u06f2\u06f3\u0005\u0002\u0000\u0000"+
		"\u06f3\u06f8\u0003p8\u0000\u06f4\u06f5\u0005\u0004\u0000\u0000\u06f5\u06f7"+
		"\u0003p8\u0000\u06f6\u06f4\u0001\u0000\u0000\u0000\u06f7\u06fa\u0001\u0000"+
		"\u0000\u0000\u06f8\u06f6\u0001\u0000\u0000\u0000\u06f8\u06f9\u0001\u0000"+
		"\u0000\u0000\u06f9\u06fb\u0001\u0000\u0000\u0000\u06fa\u06f8\u0001\u0000"+
		"\u0000\u0000\u06fb\u06fc\u0005\u0003\u0000\u0000\u06fc\u0704\u0001\u0000"+
		"\u0000\u0000\u06fd\u06fe\u0005\'\u0000\u0000\u06fe\u06ff\u0005\u0002\u0000"+
		"\u0000\u06ff\u0700\u0005\u00a8\u0000\u0000\u0700\u0701\u0005\u001c\u0000"+
		"\u0000\u0701\u0702\u0005J\u0000\u0000\u0702\u0704\u0005\u0003\u0000\u0000"+
		"\u0703\u06f1\u0001\u0000\u0000\u0000\u0703\u06fd\u0001\u0000\u0000\u0000"+
		"\u0704o\u0001\u0000\u0000\u0000\u0705\u0707\u0003\u00dam\u0000\u0706\u0708"+
		"\u0003\u00a4R\u0000\u0707\u0706\u0001\u0000\u0000\u0000\u0707\u0708\u0001"+
		"\u0000\u0000\u0000\u0708q\u0001\u0000\u0000\u0000\u0709\u070a\u0005\u0002"+
		"\u0000\u0000\u070a\u070b\u0003\u00ceg\u0000\u070b\u070c\u0005\u0004\u0000"+
		"\u0000\u070c\u0711\u0003\u00ceg\u0000\u070d\u070e\u0005\u0004\u0000\u0000"+
		"\u070e\u0710\u0003\u00ceg\u0000\u070f\u070d\u0001\u0000\u0000\u0000\u0710"+
		"\u0713\u0001\u0000\u0000\u0000\u0711\u070f\u0001\u0000\u0000\u0000\u0711"+
		"\u0712\u0001\u0000\u0000\u0000\u0712\u0714\u0001\u0000\u0000\u0000\u0713"+
		"\u0711\u0001\u0000\u0000\u0000\u0714\u0715\u0005\u0003\u0000\u0000\u0715"+
		"s\u0001\u0000\u0000\u0000\u0716\u0717\u0003v;\u0000\u0717u\u0001\u0000"+
		"\u0000\u0000\u0718\u0719\u0006;\uffff\uffff\u0000\u0719\u071b\u0003z="+
		"\u0000\u071a\u071c\u0003x<\u0000\u071b\u071a\u0001\u0000\u0000\u0000\u071b"+
		"\u071c\u0001\u0000\u0000\u0000\u071c\u0720\u0001\u0000\u0000\u0000\u071d"+
		"\u071e\u0005\u00a7\u0000\u0000\u071e\u0720\u0003v;\u0003\u071f\u0718\u0001"+
		"\u0000\u0000\u0000\u071f\u071d\u0001\u0000\u0000\u0000\u0720\u0729\u0001"+
		"\u0000\u0000\u0000\u0721\u0722\n\u0002\u0000\u0000\u0722\u0723\u0005\u0019"+
		"\u0000\u0000\u0723\u0728\u0003v;\u0003\u0724\u0725\n\u0001\u0000\u0000"+
		"\u0725\u0726\u0005\u00b3\u0000\u0000\u0726\u0728\u0003v;\u0002\u0727\u0721"+
		"\u0001\u0000\u0000\u0000\u0727\u0724\u0001\u0000\u0000\u0000\u0728\u072b"+
		"\u0001\u0000\u0000\u0000\u0729\u0727\u0001\u0000\u0000\u0000\u0729\u072a"+
		"\u0001\u0000\u0000\u0000\u072aw\u0001\u0000\u0000\u0000\u072b\u0729\u0001"+
		"\u0000\u0000\u0000\u072c\u072d\u0003\u0098L\u0000\u072d\u072e\u0003z="+
		"\u0000\u072e\u076a\u0001\u0000\u0000\u0000\u072f\u0730\u0003\u0098L\u0000"+
		"\u0730\u0731\u0003\u009aM\u0000\u0731\u0732\u0005\u0002\u0000\u0000\u0732"+
		"\u0733\u0003\f\u0006\u0000\u0733\u0734\u0005\u0003\u0000\u0000\u0734\u076a"+
		"\u0001\u0000\u0000\u0000\u0735\u0737\u0005\u00a7\u0000\u0000\u0736\u0735"+
		"\u0001\u0000\u0000\u0000\u0736\u0737\u0001\u0000\u0000\u0000\u0737\u0738"+
		"\u0001\u0000\u0000\u0000\u0738\u0739\u0005!\u0000\u0000\u0739\u073a\u0003"+
		"z=\u0000\u073a\u073b\u0005\u0019\u0000\u0000\u073b\u073c\u0003z=\u0000"+
		"\u073c\u076a\u0001\u0000\u0000\u0000\u073d\u073f\u0005\u00a7\u0000\u0000"+
		"\u073e\u073d\u0001\u0000\u0000\u0000\u073e\u073f\u0001\u0000\u0000\u0000"+
		"\u073f\u0740\u0001\u0000\u0000\u0000\u0740\u0741\u0005q\u0000\u0000\u0741"+
		"\u0742\u0005\u0002\u0000\u0000\u0742\u0747\u0003t:\u0000\u0743\u0744\u0005"+
		"\u0004\u0000\u0000\u0744\u0746\u0003t:\u0000\u0745\u0743\u0001\u0000\u0000"+
		"\u0000\u0746\u0749\u0001\u0000\u0000\u0000\u0747\u0745\u0001\u0000\u0000"+
		"\u0000\u0747\u0748\u0001\u0000\u0000\u0000\u0748\u074a\u0001\u0000\u0000"+
		"\u0000\u0749\u0747\u0001\u0000\u0000\u0000\u074a\u074b\u0005\u0003\u0000"+
		"\u0000\u074b\u076a\u0001\u0000\u0000\u0000\u074c\u074e\u0005\u00a7\u0000"+
		"\u0000\u074d\u074c\u0001\u0000\u0000\u0000\u074d\u074e\u0001\u0000\u0000"+
		"\u0000\u074e\u074f\u0001\u0000\u0000\u0000\u074f\u0750\u0005q\u0000\u0000"+
		"\u0750\u0751\u0005\u0002\u0000\u0000\u0751\u0752\u0003\f\u0006\u0000\u0752"+
		"\u0753\u0005\u0003\u0000\u0000\u0753\u076a\u0001\u0000\u0000\u0000\u0754"+
		"\u0756\u0005\u00a7\u0000\u0000\u0755\u0754\u0001\u0000\u0000\u0000\u0755"+
		"\u0756\u0001\u0000\u0000\u0000\u0756\u0757\u0001\u0000\u0000\u0000\u0757"+
		"\u0758\u0005\u008d\u0000\u0000\u0758\u075b\u0003z=\u0000\u0759\u075a\u0005"+
		"T\u0000\u0000\u075a\u075c\u0003z=\u0000\u075b\u0759\u0001\u0000\u0000"+
		"\u0000\u075b\u075c\u0001\u0000\u0000\u0000\u075c\u076a\u0001\u0000\u0000"+
		"\u0000\u075d\u075f\u0005|\u0000\u0000\u075e\u0760\u0005\u00a7\u0000\u0000"+
		"\u075f\u075e\u0001\u0000\u0000\u0000\u075f\u0760\u0001\u0000\u0000\u0000"+
		"\u0760\u0761\u0001\u0000\u0000\u0000\u0761\u076a\u0005\u00a8\u0000\u0000"+
		"\u0762\u0764\u0005|\u0000\u0000\u0763\u0765\u0005\u00a7\u0000\u0000\u0764"+
		"\u0763\u0001\u0000\u0000\u0000\u0764\u0765\u0001\u0000\u0000\u0000\u0765"+
		"\u0766\u0001\u0000\u0000\u0000\u0766\u0767\u0005K\u0000\u0000\u0767\u0768"+
		"\u0005c\u0000\u0000\u0768\u076a\u0003z=\u0000\u0769\u072c\u0001\u0000"+
		"\u0000\u0000\u0769\u072f\u0001\u0000\u0000\u0000\u0769\u0736\u0001\u0000"+
		"\u0000\u0000\u0769\u073e\u0001\u0000\u0000\u0000\u0769\u074d\u0001\u0000"+
		"\u0000\u0000\u0769\u0755\u0001\u0000\u0000\u0000\u0769\u075d\u0001\u0000"+
		"\u0000\u0000\u0769\u0762\u0001\u0000\u0000\u0000\u076ay\u0001\u0000\u0000"+
		"\u0000\u076b\u076c\u0006=\uffff\uffff\u0000\u076c\u0770\u0003|>\u0000"+
		"\u076d\u076e\u0007\u000f\u0000\u0000\u076e\u0770\u0003z=\u0004\u076f\u076b"+
		"\u0001\u0000\u0000\u0000\u076f\u076d\u0001\u0000\u0000\u0000\u0770\u077f"+
		"\u0001\u0000\u0000\u0000\u0771\u0772\n\u0003\u0000\u0000\u0772\u0773\u0007"+
		"\u0010\u0000\u0000\u0773\u077e\u0003z=\u0004\u0774\u0775\n\u0002\u0000"+
		"\u0000\u0775\u0776\u0007\u000f\u0000\u0000\u0776\u077e\u0003z=\u0003\u0777"+
		"\u0778\n\u0001\u0000\u0000\u0778\u0779\u0005\u012d\u0000\u0000\u0779\u077e"+
		"\u0003z=\u0002\u077a\u077b\n\u0005\u0000\u0000\u077b\u077c\u0005\u001e"+
		"\u0000\u0000\u077c\u077e\u0003\u0096K\u0000\u077d\u0771\u0001\u0000\u0000"+
		"\u0000\u077d\u0774\u0001\u0000\u0000\u0000\u077d\u0777\u0001\u0000\u0000"+
		"\u0000\u077d\u077a\u0001\u0000\u0000\u0000\u077e\u0781\u0001\u0000\u0000"+
		"\u0000\u077f\u077d\u0001\u0000\u0000\u0000\u077f\u0780\u0001\u0000\u0000"+
		"\u0000\u0780{\u0001\u0000\u0000\u0000\u0781\u077f\u0001\u0000\u0000\u0000"+
		"\u0782\u0783\u0006>\uffff\uffff\u0000\u0783\u0945\u0005\u00a8\u0000\u0000"+
		"\u0784\u0945\u0003\u009eO\u0000\u0785\u0786\u0003\u00dam\u0000\u0786\u0787"+
		"\u0003\u0094J\u0000\u0787\u0945\u0001\u0000\u0000\u0000\u0788\u0789\u0005"+
		"M\u0000\u0000\u0789\u078a\u0005\u00c4\u0000\u0000\u078a\u0945\u0003\u0094"+
		"J\u0000\u078b\u0945\u0003\u00dcn\u0000\u078c\u0945\u0003\u009cN\u0000"+
		"\u078d\u0945\u0003\u0094J\u0000\u078e\u0945\u0005\u0131\u0000\u0000\u078f"+
		"\u0945\u0005\u012e\u0000\u0000\u0790\u0791\u0005\u00c2\u0000\u0000\u0791"+
		"\u0792\u0005\u0002\u0000\u0000\u0792\u0793\u0003z=\u0000\u0793\u0794\u0005"+
		"q\u0000\u0000\u0794\u0795\u0003z=\u0000\u0795\u0796\u0005\u0003\u0000"+
		"\u0000\u0796\u0945\u0001\u0000\u0000\u0000\u0797\u0798\u0005\u0002\u0000"+
		"\u0000\u0798\u079b\u0003t:\u0000\u0799\u079a\u0005\u0004\u0000\u0000\u079a"+
		"\u079c\u0003t:\u0000\u079b\u0799\u0001\u0000\u0000\u0000\u079c\u079d\u0001"+
		"\u0000\u0000\u0000\u079d\u079b\u0001\u0000\u0000\u0000\u079d\u079e\u0001"+
		"\u0000\u0000\u0000\u079e\u079f\u0001\u0000\u0000\u0000\u079f\u07a0\u0005"+
		"\u0003\u0000\u0000\u07a0\u0945\u0001\u0000\u0000\u0000\u07a1\u07a2\u0005"+
		"\u00db\u0000\u0000\u07a2\u07a3\u0005\u0002\u0000\u0000\u07a3\u07a8\u0003"+
		"t:\u0000\u07a4\u07a5\u0005\u0004\u0000\u0000\u07a5\u07a7\u0003t:\u0000"+
		"\u07a6\u07a4\u0001\u0000\u0000\u0000\u07a7\u07aa\u0001\u0000\u0000\u0000"+
		"\u07a8\u07a6\u0001\u0000\u0000\u0000\u07a8\u07a9\u0001\u0000\u0000\u0000"+
		"\u07a9\u07ab\u0001\u0000\u0000\u0000\u07aa\u07a8\u0001\u0000\u0000\u0000"+
		"\u07ab\u07ac\u0005\u0003\u0000\u0000\u07ac\u0945\u0001\u0000\u0000\u0000"+
		"\u07ad\u07ae\u0005\u008f\u0000\u0000\u07ae\u07b0\u0005\u0002\u0000\u0000"+
		"\u07af\u07b1\u0003>\u001f\u0000\u07b0\u07af\u0001\u0000\u0000\u0000\u07b0"+
		"\u07b1\u0001\u0000\u0000\u0000\u07b1\u07b2\u0001\u0000\u0000\u0000\u07b2"+
		"\u07b5\u0003t:\u0000\u07b3\u07b4\u0005\u0004\u0000\u0000\u07b4\u07b6\u0003"+
		"\u0094J\u0000\u07b5\u07b3\u0001\u0000\u0000\u0000\u07b5\u07b6\u0001\u0000"+
		"\u0000\u0000\u07b6\u07ba\u0001\u0000\u0000\u0000\u07b7\u07b8\u0005\u00af"+
		"\u0000\u0000\u07b8\u07b9\u0005\u00b9\u0000\u0000\u07b9\u07bb\u0003N\'"+
		"\u0000\u07ba\u07b7\u0001\u0000\u0000\u0000\u07ba\u07bb\u0001\u0000\u0000"+
		"\u0000\u07bb\u07bc\u0001\u0000\u0000\u0000\u07bc\u07bd\u0005\u0003\u0000"+
		"\u0000\u07bd\u07be\u0005\u011b\u0000\u0000\u07be\u07bf\u0005j\u0000\u0000"+
		"\u07bf\u07c0\u0005\u0002\u0000\u0000\u07c0\u07c1\u0005\u00b4\u0000\u0000"+
		"\u07c1\u07c2\u0005#\u0000\u0000\u07c2\u07c7\u0003.\u0017\u0000\u07c3\u07c4"+
		"\u0005\u0004\u0000\u0000\u07c4\u07c6\u0003.\u0017\u0000\u07c5\u07c3\u0001"+
		"\u0000\u0000\u0000\u07c6\u07c9\u0001\u0000\u0000\u0000\u07c7\u07c5\u0001"+
		"\u0000\u0000\u0000\u07c7\u07c8\u0001\u0000\u0000\u0000\u07c8\u07ca\u0001"+
		"\u0000\u0000\u0000\u07c9\u07c7\u0001\u0000\u0000\u0000\u07ca\u07cb\u0005"+
		"\u0003\u0000\u0000\u07cb\u0945\u0001\u0000\u0000\u0000\u07cc\u07ce\u0003"+
		"\u0090H\u0000\u07cd\u07cc\u0001\u0000\u0000\u0000\u07cd\u07ce\u0001\u0000"+
		"\u0000\u0000\u07ce\u07cf\u0001\u0000\u0000\u0000\u07cf\u07d0\u0003\u00ce"+
		"g\u0000\u07d0\u07d4\u0005\u0002\u0000\u0000\u07d1\u07d2\u0003\u00dam\u0000"+
		"\u07d2\u07d3\u0005\u0001\u0000\u0000\u07d3\u07d5\u0001\u0000\u0000\u0000"+
		"\u07d4\u07d1\u0001\u0000\u0000\u0000\u07d4\u07d5\u0001\u0000\u0000\u0000"+
		"\u07d5\u07d6\u0001\u0000\u0000\u0000\u07d6\u07d7\u0005\u012a\u0000\u0000"+
		"\u07d7\u07d9\u0005\u0003\u0000\u0000\u07d8\u07da\u0003\u00acV\u0000\u07d9"+
		"\u07d8\u0001\u0000\u0000\u0000\u07d9\u07da\u0001\u0000\u0000\u0000\u07da"+
		"\u07dc\u0001\u0000\u0000\u0000\u07db\u07dd\u0003\u00b0X\u0000\u07dc\u07db"+
		"\u0001\u0000\u0000\u0000\u07dc\u07dd\u0001\u0000\u0000\u0000\u07dd\u0945"+
		"\u0001\u0000\u0000\u0000\u07de\u07e0\u0003\u0090H\u0000\u07df\u07de\u0001"+
		"\u0000\u0000\u0000\u07df\u07e0\u0001\u0000\u0000\u0000\u07e0\u07e1\u0001"+
		"\u0000\u0000\u0000\u07e1\u07e2\u0003\u00ceg\u0000\u07e2\u07ee\u0005\u0002"+
		"\u0000\u0000\u07e3\u07e5\u0003>\u001f\u0000\u07e4\u07e3\u0001\u0000\u0000"+
		"\u0000\u07e4\u07e5\u0001\u0000\u0000\u0000\u07e5\u07e6\u0001\u0000\u0000"+
		"\u0000\u07e6\u07eb\u0003t:\u0000\u07e7\u07e8\u0005\u0004\u0000\u0000\u07e8"+
		"\u07ea\u0003t:\u0000\u07e9\u07e7\u0001\u0000\u0000\u0000\u07ea\u07ed\u0001"+
		"\u0000\u0000\u0000\u07eb\u07e9\u0001\u0000\u0000\u0000\u07eb\u07ec\u0001"+
		"\u0000\u0000\u0000\u07ec\u07ef\u0001\u0000\u0000\u0000\u07ed\u07eb\u0001"+
		"\u0000\u0000\u0000\u07ee\u07e4\u0001\u0000\u0000\u0000\u07ee\u07ef\u0001"+
		"\u0000\u0000\u0000\u07ef\u07fa\u0001\u0000\u0000\u0000\u07f0\u07f1\u0005"+
		"\u00b4\u0000\u0000\u07f1\u07f2\u0005#\u0000\u0000\u07f2\u07f7\u0003.\u0017"+
		"\u0000\u07f3\u07f4\u0005\u0004\u0000\u0000\u07f4\u07f6\u0003.\u0017\u0000"+
		"\u07f5\u07f3\u0001\u0000\u0000\u0000\u07f6\u07f9\u0001\u0000\u0000\u0000"+
		"\u07f7\u07f5\u0001\u0000\u0000\u0000\u07f7\u07f8\u0001\u0000\u0000\u0000"+
		"\u07f8\u07fb\u0001\u0000\u0000\u0000\u07f9\u07f7\u0001\u0000\u0000\u0000"+
		"\u07fa\u07f0\u0001\u0000\u0000\u0000\u07fa\u07fb\u0001\u0000\u0000\u0000"+
		"\u07fb\u07fc\u0001\u0000\u0000\u0000\u07fc\u07fe\u0005\u0003\u0000\u0000"+
		"\u07fd\u07ff\u0003\u00acV\u0000\u07fe\u07fd\u0001\u0000\u0000\u0000\u07fe"+
		"\u07ff\u0001\u0000\u0000\u0000\u07ff\u0804\u0001\u0000\u0000\u0000\u0800"+
		"\u0802\u0003\u0092I\u0000\u0801\u0800\u0001\u0000\u0000\u0000\u0801\u0802"+
		"\u0001\u0000\u0000\u0000\u0802\u0803\u0001\u0000\u0000\u0000\u0803\u0805"+
		"\u0003\u00b0X\u0000\u0804\u0801\u0001\u0000\u0000\u0000\u0804\u0805\u0001"+
		"\u0000\u0000\u0000\u0805\u0945\u0001\u0000\u0000\u0000\u0806\u0807\u0003"+
		"\u00dam\u0000\u0807\u0808\u0003\u00b0X\u0000\u0808\u0945\u0001\u0000\u0000"+
		"\u0000\u0809\u080a\u0003\u00dam\u0000\u080a\u080b\u0005\u0007\u0000\u0000"+
		"\u080b\u080c\u0003t:\u0000\u080c\u0945\u0001\u0000\u0000\u0000\u080d\u0816"+
		"\u0005\u0002\u0000\u0000\u080e\u0813\u0003\u00dam\u0000\u080f\u0810\u0005"+
		"\u0004\u0000\u0000\u0810\u0812\u0003\u00dam\u0000\u0811\u080f\u0001\u0000"+
		"\u0000\u0000\u0812\u0815\u0001\u0000\u0000\u0000\u0813\u0811\u0001\u0000"+
		"\u0000\u0000\u0813\u0814\u0001\u0000\u0000\u0000\u0814\u0817\u0001\u0000"+
		"\u0000\u0000\u0815\u0813\u0001\u0000\u0000\u0000\u0816\u080e\u0001\u0000"+
		"\u0000\u0000\u0816\u0817\u0001\u0000\u0000\u0000\u0817\u0818\u0001\u0000"+
		"\u0000\u0000\u0818\u0819\u0005\u0003\u0000\u0000\u0819\u081a\u0005\u0007"+
		"\u0000\u0000\u081a\u0945\u0003t:\u0000\u081b\u081c\u0005\u0002\u0000\u0000"+
		"\u081c\u081d\u0003\f\u0006\u0000\u081d\u081e\u0005\u0003\u0000\u0000\u081e"+
		"\u0945\u0001\u0000\u0000\u0000\u081f\u0820\u0005X\u0000\u0000\u0820\u0821"+
		"\u0005\u0002\u0000\u0000\u0821\u0822\u0003\f\u0006\u0000\u0822\u0823\u0005"+
		"\u0003\u0000\u0000\u0823\u0945\u0001\u0000\u0000\u0000\u0824\u0825\u0005"+
		"&\u0000\u0000\u0825\u0827\u0003t:\u0000\u0826\u0828\u0003\u00aaU\u0000"+
		"\u0827\u0826\u0001\u0000\u0000\u0000\u0828\u0829\u0001\u0000\u0000\u0000"+
		"\u0829\u0827\u0001\u0000\u0000\u0000\u0829\u082a\u0001\u0000\u0000\u0000"+
		"\u082a\u082d\u0001\u0000\u0000\u0000\u082b\u082c\u0005O\u0000\u0000\u082c"+
		"\u082e\u0003t:\u0000\u082d\u082b\u0001\u0000\u0000\u0000\u082d\u082e\u0001"+
		"\u0000\u0000\u0000\u082e\u082f\u0001\u0000\u0000\u0000\u082f\u0830\u0005"+
		"R\u0000\u0000\u0830\u0945\u0001\u0000\u0000\u0000\u0831\u0833\u0005&\u0000"+
		"\u0000\u0832\u0834\u0003\u00aaU\u0000\u0833\u0832\u0001\u0000\u0000\u0000"+
		"\u0834\u0835\u0001\u0000\u0000\u0000\u0835\u0833\u0001\u0000\u0000\u0000"+
		"\u0835\u0836\u0001\u0000\u0000\u0000\u0836\u0839\u0001\u0000\u0000\u0000"+
		"\u0837\u0838\u0005O\u0000\u0000\u0838\u083a\u0003t:\u0000\u0839\u0837"+
		"\u0001\u0000\u0000\u0000\u0839\u083a\u0001\u0000\u0000\u0000\u083a\u083b"+
		"\u0001\u0000\u0000\u0000\u083b\u083c\u0005R\u0000\u0000\u083c\u0945\u0001"+
		"\u0000\u0000\u0000\u083d\u083e\u0005\'\u0000\u0000\u083e\u083f\u0005\u0002"+
		"\u0000\u0000\u083f\u0840\u0003t:\u0000\u0840\u0841\u0005\u001c\u0000\u0000"+
		"\u0841\u0842\u0003\u00a4R\u0000\u0842\u0843\u0005\u0003\u0000\u0000\u0843"+
		"\u0945\u0001\u0000\u0000\u0000\u0844\u0845\u0005\u00ff\u0000\u0000\u0845"+
		"\u0846\u0005\u0002\u0000\u0000\u0846\u0847\u0003t:\u0000\u0847\u0848\u0005"+
		"\u001c\u0000\u0000\u0848\u0849\u0003\u00a4R\u0000\u0849\u084a\u0005\u0003"+
		"\u0000\u0000\u084a\u0945\u0001\u0000\u0000\u0000\u084b\u084c\u0005\u001b"+
		"\u0000\u0000\u084c\u0855\u0005\b\u0000\u0000\u084d\u0852\u0003t:\u0000"+
		"\u084e\u084f\u0005\u0004\u0000\u0000\u084f\u0851\u0003t:\u0000\u0850\u084e"+
		"\u0001\u0000\u0000\u0000\u0851\u0854\u0001\u0000\u0000\u0000\u0852\u0850"+
		"\u0001\u0000\u0000\u0000\u0852\u0853\u0001\u0000\u0000\u0000\u0853\u0856"+
		"\u0001\u0000\u0000\u0000\u0854\u0852\u0001\u0000\u0000\u0000\u0855\u084d"+
		"\u0001\u0000\u0000\u0000\u0855\u0856\u0001\u0000\u0000\u0000\u0856\u0857"+
		"\u0001\u0000\u0000\u0000\u0857\u0945\u0005\t\u0000\u0000\u0858\u0945\u0003"+
		"\u00dam\u0000\u0859\u0945\u00058\u0000\u0000\u085a\u085e\u0005<\u0000"+
		"\u0000\u085b\u085c\u0005\u0002\u0000\u0000\u085c\u085d\u0005\u0132\u0000"+
		"\u0000\u085d\u085f\u0005\u0003\u0000\u0000\u085e\u085b\u0001\u0000\u0000"+
		"\u0000\u085e\u085f\u0001\u0000\u0000\u0000\u085f\u0945\u0001\u0000\u0000"+
		"\u0000\u0860\u0864\u0005=\u0000\u0000\u0861\u0862\u0005\u0002\u0000\u0000"+
		"\u0862\u0863\u0005\u0132\u0000\u0000\u0863\u0865\u0005\u0003\u0000\u0000"+
		"\u0864\u0861\u0001\u0000\u0000\u0000\u0864\u0865\u0001\u0000\u0000\u0000"+
		"\u0865\u0945\u0001\u0000\u0000\u0000\u0866\u086a\u0005\u0091\u0000\u0000"+
		"\u0867\u0868\u0005\u0002\u0000\u0000\u0868\u0869\u0005\u0132\u0000\u0000"+
		"\u0869\u086b\u0005\u0003\u0000\u0000\u086a\u0867\u0001\u0000\u0000\u0000"+
		"\u086a\u086b\u0001\u0000\u0000\u0000\u086b\u0945\u0001\u0000\u0000\u0000"+
		"\u086c\u0870\u0005\u0092\u0000\u0000\u086d\u086e\u0005\u0002\u0000\u0000"+
		"\u086e\u086f\u0005\u0132\u0000\u0000\u086f\u0871\u0005\u0003\u0000\u0000"+
		"\u0870\u086d\u0001\u0000\u0000\u0000\u0870\u0871\u0001\u0000\u0000\u0000"+
		"\u0871\u0945\u0001\u0000\u0000\u0000\u0872\u0945\u0005>\u0000\u0000\u0873"+
		"\u0945\u00057\u0000\u0000\u0874\u0945\u0005;\u0000\u0000\u0875\u0945\u0005"+
		"9\u0000\u0000\u0876\u0877\u0005\u00fc\u0000\u0000\u0877\u087f\u0005\u0002"+
		"\u0000\u0000\u0878\u087a\u0003L&\u0000\u0879\u0878\u0001\u0000\u0000\u0000"+
		"\u0879\u087a\u0001\u0000\u0000\u0000\u087a\u087c\u0001\u0000\u0000\u0000"+
		"\u087b\u087d\u0003z=\u0000\u087c\u087b\u0001\u0000\u0000\u0000\u087c\u087d"+
		"\u0001\u0000\u0000\u0000\u087d\u087e\u0001\u0000\u0000\u0000\u087e\u0880"+
		"\u0005c\u0000\u0000\u087f\u0879\u0001\u0000\u0000\u0000\u087f\u0880\u0001"+
		"\u0000\u0000\u0000\u0880\u0881\u0001\u0000\u0000\u0000\u0881\u0882\u0003"+
		"z=\u0000\u0882\u0883\u0005\u0003\u0000\u0000\u0883\u0945\u0001\u0000\u0000"+
		"\u0000\u0884\u0885\u0005\u00fc\u0000\u0000\u0885\u0886\u0005\u0002\u0000"+
		"\u0000\u0886\u0887\u0003z=\u0000\u0887\u0888\u0005\u0004\u0000\u0000\u0888"+
		"\u0889\u0003z=\u0000\u0889\u088a\u0005\u0003\u0000\u0000\u088a\u0945\u0001"+
		"\u0000\u0000\u0000\u088b\u088c\u0005\u00ee\u0000\u0000\u088c\u088d\u0005"+
		"\u0002\u0000\u0000\u088d\u088e\u0003z=\u0000\u088e\u088f\u0005c\u0000"+
		"\u0000\u088f\u0892\u0003z=\u0000\u0890\u0891\u0005a\u0000\u0000\u0891"+
		"\u0893\u0003z=\u0000\u0892\u0890\u0001\u0000\u0000\u0000\u0892\u0893\u0001"+
		"\u0000\u0000\u0000\u0893\u0894\u0001\u0000\u0000\u0000\u0894\u0895\u0005"+
		"\u0003\u0000\u0000\u0895\u0945\u0001\u0000\u0000\u0000\u0896\u0897\u0005"+
		"\u00a6\u0000\u0000\u0897\u0898\u0005\u0002\u0000\u0000\u0898\u089b\u0003"+
		"z=\u0000\u0899\u089a\u0005\u0004\u0000\u0000\u089a\u089c\u0003\u00a2Q"+
		"\u0000\u089b\u0899\u0001\u0000\u0000\u0000\u089b\u089c\u0001\u0000\u0000"+
		"\u0000\u089c\u089d\u0001\u0000\u0000\u0000\u089d\u089e\u0005\u0003\u0000"+
		"\u0000\u089e\u0945\u0001\u0000\u0000\u0000\u089f\u08a0\u0005Z\u0000\u0000"+
		"\u08a0\u08a1\u0005\u0002\u0000\u0000\u08a1\u08a2\u0003\u00dam\u0000\u08a2"+
		"\u08a3\u0005c\u0000\u0000\u08a3\u08a4\u0003z=\u0000\u08a4\u08a5\u0005"+
		"\u0003\u0000\u0000\u08a5\u0945\u0001\u0000\u0000\u0000\u08a6\u08a7\u0005"+
		"\u0002\u0000\u0000\u08a7\u08a8\u0003t:\u0000\u08a8\u08a9\u0005\u0003\u0000"+
		"\u0000\u08a9\u0945\u0001\u0000\u0000\u0000\u08aa\u08ab\u0005k\u0000\u0000"+
		"\u08ab\u08b4\u0005\u0002\u0000\u0000\u08ac\u08b1\u0003\u00ce";
	private static final String _serializedATNSegment1 =
		"g\u0000\u08ad\u08ae\u0005\u0004\u0000\u0000\u08ae\u08b0\u0003\u00ceg\u0000"+
		"\u08af\u08ad\u0001\u0000\u0000\u0000\u08b0\u08b3\u0001\u0000\u0000\u0000"+
		"\u08b1\u08af\u0001\u0000\u0000\u0000\u08b1\u08b2\u0001\u0000\u0000\u0000"+
		"\u08b2\u08b5\u0001\u0000\u0000\u0000\u08b3\u08b1\u0001\u0000\u0000\u0000"+
		"\u08b4\u08ac\u0001\u0000\u0000\u0000\u08b4\u08b5\u0001\u0000\u0000\u0000"+
		"\u08b5\u08b6\u0001\u0000\u0000\u0000\u08b6\u0945\u0005\u0003\u0000\u0000"+
		"\u08b7\u08b8\u0005\u0081\u0000\u0000\u08b8\u08b9\u0005\u0002\u0000\u0000"+
		"\u08b9\u08be\u0003~?\u0000\u08ba\u08bb\u0003\u0086C\u0000\u08bb\u08bc"+
		"\u0005\u00af\u0000\u0000\u08bc\u08bd\u0005S\u0000\u0000\u08bd\u08bf\u0001"+
		"\u0000\u0000\u0000\u08be\u08ba\u0001\u0000\u0000\u0000\u08be\u08bf\u0001"+
		"\u0000\u0000\u0000\u08bf\u08c0\u0001\u0000\u0000\u0000\u08c0\u08c1\u0005"+
		"\u0003\u0000\u0000\u08c1\u0945\u0001\u0000\u0000\u0000\u08c2\u08c3\u0005"+
		"\u0084\u0000\u0000\u08c3\u08c4\u0005\u0002\u0000\u0000\u08c4\u08c7\u0003"+
		"~?\u0000\u08c5\u08c6\u0005\u00d4\u0000\u0000\u08c6\u08c8\u0003\u00a4R"+
		"\u0000\u08c7\u08c5\u0001\u0000\u0000\u0000\u08c7\u08c8\u0001\u0000\u0000"+
		"\u0000\u08c8\u08cd\u0001\u0000\u0000\u0000\u08c9\u08ca\u0003\u0088D\u0000"+
		"\u08ca\u08cb\u0005\u00af\u0000\u0000\u08cb\u08cc\u0005P\u0000\u0000\u08cc"+
		"\u08ce\u0001\u0000\u0000\u0000\u08cd\u08c9\u0001\u0000\u0000\u0000\u08cd"+
		"\u08ce\u0001\u0000\u0000\u0000\u08ce\u08d3\u0001\u0000\u0000\u0000\u08cf"+
		"\u08d0\u0003\u0088D\u0000\u08d0\u08d1\u0005\u00af\u0000\u0000\u08d1\u08d2"+
		"\u0005S\u0000\u0000\u08d2\u08d4\u0001\u0000\u0000\u0000\u08d3\u08cf\u0001"+
		"\u0000\u0000\u0000\u08d3\u08d4\u0001\u0000\u0000\u0000\u08d4\u08d5\u0001"+
		"\u0000\u0000\u0000\u08d5\u08d6\u0005\u0003\u0000\u0000\u08d6\u0945\u0001"+
		"\u0000\u0000\u0000\u08d7\u08d8\u0005\u0083\u0000\u0000\u08d8\u08d9\u0005"+
		"\u0002\u0000\u0000\u08d9\u08e0\u0003~?\u0000\u08da\u08db\u0005\u00d4\u0000"+
		"\u0000\u08db\u08de\u0003\u00a4R\u0000\u08dc\u08dd\u0005b\u0000\u0000\u08dd"+
		"\u08df\u0003\u0082A\u0000\u08de\u08dc\u0001\u0000\u0000\u0000\u08de\u08df"+
		"\u0001\u0000\u0000\u0000\u08df\u08e1\u0001\u0000\u0000\u0000\u08e0\u08da"+
		"\u0001\u0000\u0000\u0000\u08e0\u08e1\u0001\u0000\u0000\u0000\u08e1\u08e5"+
		"\u0001\u0000\u0000\u0000\u08e2\u08e3\u0003\u008aE\u0000\u08e3\u08e4\u0005"+
		"\u011e\u0000\u0000\u08e4\u08e6\u0001\u0000\u0000\u0000\u08e5\u08e2\u0001"+
		"\u0000\u0000\u0000\u08e5\u08e6\u0001\u0000\u0000\u0000\u08e6\u08ee\u0001"+
		"\u0000\u0000\u0000\u08e7\u08e8\u0007\u0011\u0000\u0000\u08e8\u08ec\u0005"+
		"\u00c9\u0000\u0000\u08e9\u08ea\u0005\u00af\u0000\u0000\u08ea\u08eb\u0005"+
		"\u00de\u0000\u0000\u08eb\u08ed\u0005\u00f4\u0000\u0000\u08ec\u08e9\u0001"+
		"\u0000\u0000\u0000\u08ec\u08ed\u0001\u0000\u0000\u0000\u08ed\u08ef\u0001"+
		"\u0000\u0000\u0000\u08ee\u08e7\u0001\u0000\u0000\u0000\u08ee\u08ef\u0001"+
		"\u0000\u0000\u0000\u08ef\u08f4\u0001\u0000\u0000\u0000\u08f0\u08f1\u0003"+
		"\u008cF\u0000\u08f1\u08f2\u0005\u00af\u0000\u0000\u08f2\u08f3\u0005P\u0000"+
		"\u0000\u08f3\u08f5\u0001\u0000\u0000\u0000\u08f4\u08f0\u0001\u0000\u0000"+
		"\u0000\u08f4\u08f5\u0001\u0000\u0000\u0000\u08f5\u08fa\u0001\u0000\u0000"+
		"\u0000\u08f6\u08f7\u0003\u008cF\u0000\u08f7\u08f8\u0005\u00af\u0000\u0000"+
		"\u08f8\u08f9\u0005S\u0000\u0000\u08f9\u08fb\u0001\u0000\u0000\u0000\u08fa"+
		"\u08f6\u0001\u0000\u0000\u0000\u08fa\u08fb\u0001\u0000\u0000\u0000\u08fb"+
		"\u08fc\u0001\u0000\u0000\u0000\u08fc\u08fd\u0005\u0003\u0000\u0000\u08fd"+
		"\u0945\u0001\u0000\u0000\u0000\u08fe\u08ff\u0005\u0082\u0000\u0000\u08ff"+
		"\u091c\u0005\u0002\u0000\u0000\u0900\u0905\u0003\u008eG\u0000\u0901\u0902"+
		"\u0005\u0004\u0000\u0000\u0902\u0904\u0003\u008eG\u0000\u0903\u0901\u0001"+
		"\u0000\u0000\u0000\u0904\u0907\u0001\u0000\u0000\u0000\u0905\u0903\u0001"+
		"\u0000\u0000\u0000\u0905\u0906\u0001\u0000\u0000\u0000\u0906\u090e\u0001"+
		"\u0000\u0000\u0000\u0907\u0905\u0001\u0000\u0000\u0000\u0908\u0909\u0005"+
		"\u00a8\u0000\u0000\u0909\u090a\u0005\u00af\u0000\u0000\u090a\u090f\u0005"+
		"\u00a8\u0000\u0000\u090b\u090c\u0005\u0012\u0000\u0000\u090c\u090d\u0005"+
		"\u00af\u0000\u0000\u090d\u090f\u0005\u00a8\u0000\u0000\u090e\u0908\u0001"+
		"\u0000\u0000\u0000\u090e\u090b\u0001\u0000\u0000\u0000\u090e\u090f\u0001"+
		"\u0000\u0000\u0000\u090f\u091a\u0001\u0000\u0000\u0000\u0910\u0911\u0005"+
		"\u011a\u0000\u0000\u0911\u0913\u0005\u0106\u0000\u0000\u0912\u0914\u0005"+
		"\u0087\u0000\u0000\u0913\u0912\u0001\u0000\u0000\u0000\u0913\u0914\u0001"+
		"\u0000\u0000\u0000\u0914\u091b\u0001\u0000\u0000\u0000\u0915\u0916\u0005"+
		"\u011c\u0000\u0000\u0916\u0918\u0005\u0106\u0000\u0000\u0917\u0919\u0005"+
		"\u0087\u0000\u0000\u0918\u0917\u0001\u0000\u0000\u0000\u0918\u0919\u0001"+
		"\u0000\u0000\u0000\u0919\u091b\u0001\u0000\u0000\u0000\u091a\u0910\u0001"+
		"\u0000\u0000\u0000\u091a\u0915\u0001\u0000\u0000\u0000\u091a\u091b\u0001"+
		"\u0000\u0000\u0000\u091b\u091d\u0001\u0000\u0000\u0000\u091c\u0900\u0001"+
		"\u0000\u0000\u0000\u091c\u091d\u0001\u0000\u0000\u0000\u091d\u0924\u0001"+
		"\u0000\u0000\u0000\u091e\u091f\u0005\u00d4\u0000\u0000\u091f\u0922\u0003"+
		"\u00a4R\u0000\u0920\u0921\u0005b\u0000\u0000\u0921\u0923\u0003\u0082A"+
		"\u0000\u0922\u0920\u0001\u0000\u0000\u0000\u0922\u0923\u0001\u0000\u0000"+
		"\u0000\u0923\u0925\u0001\u0000\u0000\u0000\u0924\u091e\u0001\u0000\u0000"+
		"\u0000\u0924\u0925\u0001\u0000\u0000\u0000\u0925\u0926\u0001\u0000\u0000"+
		"\u0000\u0926\u0945\u0005\u0003\u0000\u0000\u0927\u0928\u0005\u0080\u0000"+
		"\u0000\u0928\u0939\u0005\u0002\u0000\u0000\u0929\u092e\u0003\u0080@\u0000"+
		"\u092a\u092b\u0005\u0004\u0000\u0000\u092b\u092d\u0003\u0080@\u0000\u092c"+
		"\u092a\u0001\u0000\u0000\u0000\u092d\u0930\u0001\u0000\u0000\u0000\u092e"+
		"\u092c\u0001\u0000\u0000\u0000\u092e\u092f\u0001\u0000\u0000\u0000\u092f"+
		"\u0937\u0001\u0000\u0000\u0000\u0930\u092e\u0001\u0000\u0000\u0000\u0931"+
		"\u0932\u0005\u00a8\u0000\u0000\u0932\u0933\u0005\u00af\u0000\u0000\u0933"+
		"\u0938\u0005\u00a8\u0000\u0000\u0934\u0935\u0005\u0012\u0000\u0000\u0935"+
		"\u0936\u0005\u00af\u0000\u0000\u0936\u0938\u0005\u00a8\u0000\u0000\u0937"+
		"\u0931\u0001\u0000\u0000\u0000\u0937\u0934\u0001\u0000\u0000\u0000\u0937"+
		"\u0938\u0001\u0000\u0000\u0000\u0938\u093a\u0001\u0000\u0000\u0000\u0939"+
		"\u0929\u0001\u0000\u0000\u0000\u0939\u093a\u0001\u0000\u0000\u0000\u093a"+
		"\u0941\u0001\u0000\u0000\u0000\u093b\u093c\u0005\u00d4\u0000\u0000\u093c"+
		"\u093f\u0003\u00a4R\u0000\u093d\u093e\u0005b\u0000\u0000\u093e\u0940\u0003"+
		"\u0082A\u0000\u093f\u093d\u0001\u0000\u0000\u0000\u093f\u0940\u0001\u0000"+
		"\u0000\u0000\u0940\u0942\u0001\u0000\u0000\u0000\u0941\u093b\u0001\u0000"+
		"\u0000\u0000\u0941\u0942\u0001\u0000\u0000\u0000\u0942\u0943\u0001\u0000"+
		"\u0000\u0000\u0943\u0945\u0005\u0003\u0000\u0000\u0944\u0782\u0001\u0000"+
		"\u0000\u0000\u0944\u0784\u0001\u0000\u0000\u0000\u0944\u0785\u0001\u0000"+
		"\u0000\u0000\u0944\u0788\u0001\u0000\u0000\u0000\u0944\u078b\u0001\u0000"+
		"\u0000\u0000\u0944\u078c\u0001\u0000\u0000\u0000\u0944\u078d\u0001\u0000"+
		"\u0000\u0000\u0944\u078e\u0001\u0000\u0000\u0000\u0944\u078f\u0001\u0000"+
		"\u0000\u0000\u0944\u0790\u0001\u0000\u0000\u0000\u0944\u0797\u0001\u0000"+
		"\u0000\u0000\u0944\u07a1\u0001\u0000\u0000\u0000\u0944\u07ad\u0001\u0000"+
		"\u0000\u0000\u0944\u07cd\u0001\u0000\u0000\u0000\u0944\u07df\u0001\u0000"+
		"\u0000\u0000\u0944\u0806\u0001\u0000\u0000\u0000\u0944\u0809\u0001\u0000"+
		"\u0000\u0000\u0944\u080d\u0001\u0000\u0000\u0000\u0944\u081b\u0001\u0000"+
		"\u0000\u0000\u0944\u081f\u0001\u0000\u0000\u0000\u0944\u0824\u0001\u0000"+
		"\u0000\u0000\u0944\u0831\u0001\u0000\u0000\u0000\u0944\u083d\u0001\u0000"+
		"\u0000\u0000\u0944\u0844\u0001\u0000\u0000\u0000\u0944\u084b\u0001\u0000"+
		"\u0000\u0000\u0944\u0858\u0001\u0000\u0000\u0000\u0944\u0859\u0001\u0000"+
		"\u0000\u0000\u0944\u085a\u0001\u0000\u0000\u0000\u0944\u0860\u0001\u0000"+
		"\u0000\u0000\u0944\u0866\u0001\u0000\u0000\u0000\u0944\u086c\u0001\u0000"+
		"\u0000\u0000\u0944\u0872\u0001\u0000\u0000\u0000\u0944\u0873\u0001\u0000"+
		"\u0000\u0000\u0944\u0874\u0001\u0000\u0000\u0000\u0944\u0875\u0001\u0000"+
		"\u0000\u0000\u0944\u0876\u0001\u0000\u0000\u0000\u0944\u0884\u0001\u0000"+
		"\u0000\u0000\u0944\u088b\u0001\u0000\u0000\u0000\u0944\u0896\u0001\u0000"+
		"\u0000\u0000\u0944\u089f\u0001\u0000\u0000\u0000\u0944\u08a6\u0001\u0000"+
		"\u0000\u0000\u0944\u08aa\u0001\u0000\u0000\u0000\u0944\u08b7\u0001\u0000"+
		"\u0000\u0000\u0944\u08c2\u0001\u0000\u0000\u0000\u0944\u08d7\u0001\u0000"+
		"\u0000\u0000\u0944\u08fe\u0001\u0000\u0000\u0000\u0944\u0927\u0001\u0000"+
		"\u0000\u0000\u0945\u0950\u0001\u0000\u0000\u0000\u0946\u0947\n\u0018\u0000"+
		"\u0000\u0947\u0948\u0005\b\u0000\u0000\u0948\u0949\u0003z=\u0000\u0949"+
		"\u094a\u0005\t\u0000\u0000\u094a\u094f\u0001\u0000\u0000\u0000\u094b\u094c"+
		"\n\u0016\u0000\u0000\u094c\u094d\u0005\u0001\u0000\u0000\u094d\u094f\u0003"+
		"\u00dam\u0000\u094e\u0946\u0001\u0000\u0000\u0000\u094e\u094b\u0001\u0000"+
		"\u0000\u0000\u094f\u0952\u0001\u0000\u0000\u0000\u0950\u094e\u0001\u0000"+
		"\u0000\u0000\u0950\u0951\u0001\u0000\u0000\u0000\u0951}\u0001\u0000\u0000"+
		"\u0000\u0952\u0950\u0001\u0000\u0000\u0000\u0953\u0954\u0003\u0080@\u0000"+
		"\u0954\u0955\u0005\u0004\u0000\u0000\u0955\u095f\u0003\u0094J\u0000\u0956"+
		"\u0957\u0005\u00bc\u0000\u0000\u0957\u095c\u0003\u0084B\u0000\u0958\u0959"+
		"\u0005\u0004\u0000\u0000\u0959\u095b\u0003\u0084B\u0000\u095a\u0958\u0001"+
		"\u0000\u0000\u0000\u095b\u095e\u0001\u0000\u0000\u0000\u095c\u095a\u0001"+
		"\u0000\u0000\u0000\u095c\u095d\u0001\u0000\u0000\u0000\u095d\u0960\u0001"+
		"\u0000\u0000\u0000\u095e\u095c\u0001\u0000\u0000\u0000\u095f\u0956\u0001"+
		"\u0000\u0000\u0000\u095f\u0960\u0001\u0000\u0000\u0000\u0960\u007f\u0001"+
		"\u0000\u0000\u0000\u0961\u0964\u0003t:\u0000\u0962\u0963\u0005b\u0000"+
		"\u0000\u0963\u0965\u0003\u0082A\u0000\u0964\u0962\u0001\u0000\u0000\u0000"+
		"\u0964\u0965\u0001\u0000\u0000\u0000\u0965\u0081\u0001\u0000\u0000\u0000"+
		"\u0966\u0969\u0005\u007f\u0000\u0000\u0967\u0968\u0005Q\u0000\u0000\u0968"+
		"\u096a\u0007\u0012\u0000\u0000\u0969\u0967\u0001\u0000\u0000\u0000\u0969"+
		"\u096a\u0001\u0000\u0000\u0000\u096a\u0083\u0001\u0000\u0000\u0000\u096b"+
		"\u096c\u0003\u0080@\u0000\u096c\u096d\u0005\u001c\u0000\u0000\u096d\u096e"+
		"\u0003\u00dam\u0000\u096e\u0085\u0001\u0000\u0000\u0000\u096f\u0970\u0007"+
		"\u0013\u0000\u0000\u0970\u0087\u0001\u0000\u0000\u0000\u0971\u0976\u0005"+
		"S\u0000\u0000\u0972\u0976\u0005\u00a8\u0000\u0000\u0973\u0974\u0005C\u0000"+
		"\u0000\u0974\u0976\u0003t:\u0000\u0975\u0971\u0001\u0000\u0000\u0000\u0975"+
		"\u0972\u0001\u0000\u0000\u0000\u0975\u0973\u0001\u0000\u0000\u0000\u0976"+
		"\u0089\u0001\u0000\u0000\u0000\u0977\u0979\u0005\u011c\u0000\u0000\u0978"+
		"\u097a\u0005\u001b\u0000\u0000\u0979\u0978\u0001\u0000\u0000\u0000\u0979"+
		"\u097a\u0001\u0000\u0000\u0000\u097a\u0983\u0001\u0000\u0000\u0000\u097b"+
		"\u097d\u0005\u011a\u0000\u0000\u097c\u097e\u0007\u0014\u0000\u0000\u097d"+
		"\u097c\u0001\u0000\u0000\u0000\u097d\u097e\u0001\u0000\u0000\u0000\u097e"+
		"\u0980\u0001\u0000\u0000\u0000\u097f\u0981\u0005\u001b\u0000\u0000\u0980"+
		"\u097f\u0001\u0000\u0000\u0000\u0980\u0981\u0001\u0000\u0000\u0000\u0981"+
		"\u0983\u0001\u0000\u0000\u0000\u0982\u0977\u0001\u0000\u0000\u0000\u0982"+
		"\u097b\u0001\u0000\u0000\u0000\u0983\u008b\u0001\u0000\u0000\u0000\u0984"+
		"\u098b\u0005S\u0000\u0000\u0985\u098b\u0005\u00a8\u0000\u0000\u0986\u0987"+
		"\u0005P\u0000\u0000\u0987\u098b\u0005\u001b\u0000\u0000\u0988\u0989\u0005"+
		"P\u0000\u0000\u0989\u098b\u0005\u00ab\u0000\u0000\u098a\u0984\u0001\u0000"+
		"\u0000\u0000\u098a\u0985\u0001\u0000\u0000\u0000\u098a\u0986\u0001\u0000"+
		"\u0000\u0000\u098a\u0988\u0001\u0000\u0000\u0000\u098b\u008d\u0001\u0000"+
		"\u0000\u0000\u098c\u098e\u0005\u0086\u0000\u0000\u098d\u098c\u0001\u0000"+
		"\u0000\u0000\u098d\u098e\u0001\u0000\u0000\u0000\u098e\u098f\u0001\u0000"+
		"\u0000\u0000\u098f\u0990\u0003t:\u0000\u0990\u0991\u0005\u0112\u0000\u0000"+
		"\u0991\u0992\u0003\u0080@\u0000\u0992\u0998\u0001\u0000\u0000\u0000\u0993"+
		"\u0994\u0003t:\u0000\u0994\u0995\u0005\n\u0000\u0000\u0995\u0996\u0003"+
		"\u0080@\u0000\u0996\u0998\u0001\u0000\u0000\u0000\u0997\u098d\u0001\u0000"+
		"\u0000\u0000\u0997\u0993\u0001\u0000\u0000\u0000\u0998\u008f\u0001\u0000"+
		"\u0000\u0000\u0999\u099a\u0007\u0015\u0000\u0000\u099a\u0091\u0001\u0000"+
		"\u0000\u0000\u099b\u099c\u0005p\u0000\u0000\u099c\u09a0\u0005\u00aa\u0000"+
		"\u0000\u099d\u099e\u0005\u00d2\u0000\u0000\u099e\u09a0\u0005\u00aa\u0000"+
		"\u0000\u099f\u099b\u0001\u0000\u0000\u0000\u099f\u099d\u0001\u0000\u0000"+
		"\u0000\u09a0\u0093\u0001\u0000\u0000\u0000\u09a1\u09a8\u0005\u012f\u0000"+
		"\u0000\u09a2\u09a5\u0005\u0130\u0000\u0000\u09a3\u09a4\u0005\u0101\u0000"+
		"\u0000\u09a4\u09a6\u0005\u012f\u0000\u0000\u09a5\u09a3\u0001\u0000\u0000"+
		"\u0000\u09a5\u09a6\u0001\u0000\u0000\u0000\u09a6\u09a8\u0001\u0000\u0000"+
		"\u0000\u09a7\u09a1\u0001\u0000\u0000\u0000\u09a7\u09a2\u0001\u0000\u0000"+
		"\u0000\u09a8\u0095\u0001\u0000\u0000\u0000\u09a9\u09aa\u0005\u00f7\u0000"+
		"\u0000\u09aa\u09ab\u0005\u0121\u0000\u0000\u09ab\u09b0\u0003\u009eO\u0000"+
		"\u09ac\u09ad\u0005\u00f7\u0000\u0000\u09ad\u09ae\u0005\u0121\u0000\u0000"+
		"\u09ae\u09b0\u0003\u0094J\u0000\u09af\u09a9\u0001\u0000\u0000\u0000\u09af"+
		"\u09ac\u0001\u0000\u0000\u0000\u09b0\u0097\u0001\u0000\u0000\u0000\u09b1"+
		"\u09b2\u0007\u0016\u0000\u0000\u09b2\u0099\u0001\u0000\u0000\u0000\u09b3"+
		"\u09b4\u0007\u0017\u0000\u0000\u09b4\u009b\u0001\u0000\u0000\u0000\u09b5"+
		"\u09b6\u0007\u0018\u0000\u0000\u09b6\u009d\u0001\u0000\u0000\u0000\u09b7"+
		"\u09b9\u0005x\u0000\u0000\u09b8\u09ba\u0007\u000f\u0000\u0000\u09b9\u09b8"+
		"\u0001\u0000\u0000\u0000\u09b9\u09ba\u0001\u0000\u0000\u0000\u09ba\u09bb"+
		"\u0001\u0000\u0000\u0000\u09bb\u09bc\u0003\u0094J\u0000\u09bc\u09bf\u0003"+
		"\u00a0P\u0000\u09bd\u09be\u0005\u00f9\u0000\u0000\u09be\u09c0\u0003\u00a0"+
		"P\u0000\u09bf\u09bd\u0001\u0000\u0000\u0000\u09bf\u09c0\u0001\u0000\u0000"+
		"\u0000\u09c0\u009f\u0001\u0000\u0000\u0000\u09c1\u09c2\u0007\u0019\u0000"+
		"\u0000\u09c2\u00a1\u0001\u0000\u0000\u0000\u09c3\u09c4\u0007\u001a\u0000"+
		"\u0000\u09c4\u00a3\u0001\u0000\u0000\u0000\u09c5\u09c6\u0006R\uffff\uffff"+
		"\u0000\u09c6\u09c7\u0005\u00db\u0000\u0000\u09c7\u09c8\u0005\u0002\u0000"+
		"\u0000\u09c8\u09cd\u0003\u00a6S\u0000\u09c9\u09ca\u0005\u0004\u0000\u0000"+
		"\u09ca\u09cc\u0003\u00a6S\u0000\u09cb\u09c9\u0001\u0000\u0000\u0000\u09cc"+
		"\u09cf\u0001\u0000\u0000\u0000\u09cd\u09cb\u0001\u0000\u0000\u0000\u09cd"+
		"\u09ce\u0001\u0000\u0000\u0000\u09ce\u09d0\u0001\u0000\u0000\u0000\u09cf"+
		"\u09cd\u0001\u0000\u0000\u0000\u09d0\u09d1\u0005\u0003\u0000\u0000\u09d1"+
		"\u0a21\u0001\u0000\u0000\u0000\u09d2\u09d3\u0005x\u0000\u0000\u09d3\u09d6"+
		"\u0003\u00a0P\u0000\u09d4\u09d5\u0005\u00f9\u0000\u0000\u09d5\u09d7\u0003"+
		"\u00a0P\u0000\u09d6\u09d4\u0001\u0000\u0000\u0000\u09d6\u09d7\u0001\u0000"+
		"\u0000\u0000\u09d7\u0a21\u0001\u0000\u0000\u0000\u09d8\u09dd\u0005\u00f8"+
		"\u0000\u0000\u09d9\u09da\u0005\u0002\u0000\u0000\u09da\u09db\u0003\u00a8"+
		"T\u0000\u09db\u09dc\u0005\u0003\u0000\u0000\u09dc\u09de\u0001\u0000\u0000"+
		"\u0000\u09dd\u09d9\u0001\u0000\u0000\u0000\u09dd\u09de\u0001\u0000\u0000"+
		"\u0000\u09de\u09e2\u0001\u0000\u0000\u0000\u09df\u09e0\u0005\u011c\u0000"+
		"\u0000\u09e0\u09e1\u0005\u00f7\u0000\u0000\u09e1\u09e3\u0005\u0121\u0000"+
		"\u0000\u09e2\u09df\u0001\u0000\u0000\u0000\u09e2\u09e3\u0001\u0000\u0000"+
		"\u0000\u09e3\u0a21\u0001\u0000\u0000\u0000\u09e4\u09e9\u0005\u00f8\u0000"+
		"\u0000\u09e5\u09e6\u0005\u0002\u0000\u0000\u09e6\u09e7\u0003\u00a8T\u0000"+
		"\u09e7\u09e8\u0005\u0003\u0000\u0000\u09e8\u09ea\u0001\u0000\u0000\u0000"+
		"\u09e9\u09e5\u0001\u0000\u0000\u0000\u09e9\u09ea\u0001\u0000\u0000\u0000"+
		"\u09ea\u09eb\u0001\u0000\u0000\u0000\u09eb\u09ec\u0005\u011a\u0000\u0000"+
		"\u09ec\u09ed\u0005\u00f7\u0000\u0000\u09ed\u0a21\u0005\u0121\u0000\u0000"+
		"\u09ee\u09f3\u0005\u00f7\u0000\u0000\u09ef\u09f0\u0005\u0002\u0000\u0000"+
		"\u09f0\u09f1\u0003\u00a8T\u0000\u09f1\u09f2\u0005\u0003\u0000\u0000\u09f2"+
		"\u09f4\u0001\u0000\u0000\u0000\u09f3\u09ef\u0001\u0000\u0000\u0000\u09f3"+
		"\u09f4\u0001\u0000\u0000\u0000\u09f4\u09f8\u0001\u0000\u0000\u0000\u09f5"+
		"\u09f6\u0005\u011c\u0000\u0000\u09f6\u09f7\u0005\u00f7\u0000\u0000\u09f7"+
		"\u09f9\u0005\u0121\u0000\u0000\u09f8\u09f5\u0001\u0000\u0000\u0000\u09f8"+
		"\u09f9\u0001\u0000\u0000\u0000\u09f9\u0a21\u0001\u0000\u0000\u0000\u09fa"+
		"\u09ff\u0005\u00f7\u0000\u0000\u09fb\u09fc\u0005\u0002\u0000\u0000\u09fc"+
		"\u09fd\u0003\u00a8T\u0000\u09fd\u09fe\u0005\u0003\u0000\u0000\u09fe\u0a00"+
		"\u0001\u0000\u0000\u0000\u09ff\u09fb\u0001\u0000\u0000\u0000\u09ff\u0a00"+
		"\u0001\u0000\u0000\u0000\u0a00\u0a01\u0001\u0000\u0000\u0000\u0a01\u0a02"+
		"\u0005\u011a\u0000\u0000\u0a02\u0a03\u0005\u00f7\u0000\u0000\u0a03\u0a21"+
		"\u0005\u0121\u0000\u0000\u0a04\u0a05\u0005M\u0000\u0000\u0a05\u0a21\u0005"+
		"\u00c4\u0000\u0000\u0a06\u0a07\u0005\u001b\u0000\u0000\u0a07\u0a08\u0005"+
		"\u0124\u0000\u0000\u0a08\u0a09\u0003\u00a4R\u0000\u0a09\u0a0a\u0005\u0126"+
		"\u0000\u0000\u0a0a\u0a21\u0001\u0000\u0000\u0000\u0a0b\u0a0c\u0005\u0094"+
		"\u0000\u0000\u0a0c\u0a0d\u0005\u0124\u0000\u0000\u0a0d\u0a0e\u0003\u00a4"+
		"R\u0000\u0a0e\u0a0f\u0005\u0004\u0000\u0000\u0a0f\u0a10\u0003\u00a4R\u0000"+
		"\u0a10\u0a11\u0005\u0126\u0000\u0000\u0a11\u0a21\u0001\u0000\u0000\u0000"+
		"\u0a12\u0a1e\u0003\u00dam\u0000\u0a13\u0a14\u0005\u0002\u0000\u0000\u0a14"+
		"\u0a19\u0003\u00a8T\u0000\u0a15\u0a16\u0005\u0004\u0000\u0000\u0a16\u0a18"+
		"\u0003\u00a8T\u0000\u0a17\u0a15\u0001\u0000\u0000\u0000\u0a18\u0a1b\u0001"+
		"\u0000\u0000\u0000\u0a19\u0a17\u0001\u0000\u0000\u0000\u0a19\u0a1a\u0001"+
		"\u0000\u0000\u0000\u0a1a\u0a1c\u0001\u0000\u0000\u0000\u0a1b\u0a19\u0001"+
		"\u0000\u0000\u0000\u0a1c\u0a1d\u0005\u0003\u0000\u0000\u0a1d\u0a1f\u0001"+
		"\u0000\u0000\u0000\u0a1e\u0a13\u0001\u0000\u0000\u0000\u0a1e\u0a1f\u0001"+
		"\u0000\u0000\u0000\u0a1f\u0a21\u0001\u0000\u0000\u0000\u0a20\u09c5\u0001"+
		"\u0000\u0000\u0000\u0a20\u09d2\u0001\u0000\u0000\u0000\u0a20\u09d8\u0001"+
		"\u0000\u0000\u0000\u0a20\u09e4\u0001\u0000\u0000\u0000\u0a20\u09ee\u0001"+
		"\u0000\u0000\u0000\u0a20\u09fa\u0001\u0000\u0000\u0000\u0a20\u0a04\u0001"+
		"\u0000\u0000\u0000\u0a20\u0a06\u0001\u0000\u0000\u0000\u0a20\u0a0b\u0001"+
		"\u0000\u0000\u0000\u0a20\u0a12\u0001\u0000\u0000\u0000\u0a21\u0a2b\u0001"+
		"\u0000\u0000\u0000\u0a22\u0a23\n\u0002\u0000\u0000\u0a23\u0a27\u0005\u001b"+
		"\u0000\u0000\u0a24\u0a25\u0005\b\u0000\u0000\u0a25\u0a26\u0005\u0132\u0000"+
		"\u0000\u0a26\u0a28\u0005\t\u0000\u0000\u0a27\u0a24\u0001\u0000\u0000\u0000"+
		"\u0a27\u0a28\u0001\u0000\u0000\u0000\u0a28\u0a2a\u0001\u0000\u0000\u0000"+
		"\u0a29\u0a22\u0001\u0000\u0000\u0000\u0a2a\u0a2d\u0001\u0000\u0000\u0000"+
		"\u0a2b\u0a29\u0001\u0000\u0000\u0000\u0a2b\u0a2c\u0001\u0000\u0000\u0000"+
		"\u0a2c\u00a5\u0001\u0000\u0000\u0000\u0a2d\u0a2b\u0001\u0000\u0000\u0000"+
		"\u0a2e\u0a33\u0003\u00a4R\u0000\u0a2f\u0a30\u0003\u00dam\u0000\u0a30\u0a31"+
		"\u0003\u00a4R\u0000\u0a31\u0a33\u0001\u0000\u0000\u0000\u0a32\u0a2e\u0001"+
		"\u0000\u0000\u0000\u0a32\u0a2f\u0001\u0000\u0000\u0000\u0a33\u00a7\u0001"+
		"\u0000\u0000\u0000\u0a34\u0a37\u0005\u0132\u0000\u0000\u0a35\u0a37\u0003"+
		"\u00a4R\u0000\u0a36\u0a34\u0001\u0000\u0000\u0000\u0a36\u0a35\u0001\u0000"+
		"\u0000\u0000\u0a37\u00a9\u0001\u0000\u0000\u0000\u0a38\u0a39\u0005\u0117"+
		"\u0000\u0000\u0a39\u0a3a\u0003t:\u0000\u0a3a\u0a3b\u0005\u00f5\u0000\u0000"+
		"\u0a3b\u0a3c\u0003t:\u0000\u0a3c\u00ab\u0001\u0000\u0000\u0000\u0a3d\u0a3e"+
		"\u0005]\u0000\u0000\u0a3e\u0a3f\u0005\u0002\u0000\u0000\u0a3f\u0a40\u0005"+
		"\u0118\u0000\u0000\u0a40\u0a41\u0003v;\u0000\u0a41\u0a42\u0005\u0003\u0000"+
		"\u0000\u0a42\u00ad\u0001\u0000\u0000\u0000\u0a43\u0a44\u0005\u0117\u0000"+
		"\u0000\u0a44\u0a47\u0005\u0096\u0000\u0000\u0a45\u0a46\u0005\u0019\u0000"+
		"\u0000\u0a46\u0a48\u0003t:\u0000\u0a47\u0a45\u0001\u0000\u0000\u0000\u0a47"+
		"\u0a48\u0001\u0000\u0000\u0000\u0a48\u0a49\u0001\u0000\u0000\u0000\u0a49"+
		"\u0a4a\u0005\u00f5\u0000\u0000\u0a4a\u0a4b\u0005\u010a\u0000\u0000\u0a4b"+
		"\u0a4c\u0005\u00e7\u0000\u0000\u0a4c\u0a4d\u0003\u00dam\u0000\u0a4d\u0a4e"+
		"\u0005\u0122\u0000\u0000\u0a4e\u0a56\u0003t:\u0000\u0a4f\u0a50\u0005\u0004"+
		"\u0000\u0000\u0a50\u0a51\u0003\u00dam\u0000\u0a51\u0a52\u0005\u0122\u0000"+
		"\u0000\u0a52\u0a53\u0003t:\u0000\u0a53\u0a55\u0001\u0000\u0000\u0000\u0a54"+
		"\u0a4f\u0001\u0000\u0000\u0000\u0a55\u0a58\u0001\u0000\u0000\u0000\u0a56"+
		"\u0a54\u0001\u0000\u0000\u0000\u0a56\u0a57\u0001\u0000\u0000\u0000\u0a57"+
		"\u0a84\u0001\u0000\u0000\u0000\u0a58\u0a56\u0001\u0000\u0000\u0000\u0a59"+
		"\u0a5a\u0005\u0117\u0000\u0000\u0a5a\u0a5d\u0005\u0096\u0000\u0000\u0a5b"+
		"\u0a5c\u0005\u0019\u0000\u0000\u0a5c\u0a5e\u0003t:\u0000\u0a5d\u0a5b\u0001"+
		"\u0000\u0000\u0000\u0a5d\u0a5e\u0001\u0000\u0000\u0000\u0a5e\u0a5f\u0001"+
		"\u0000\u0000\u0000\u0a5f\u0a60\u0005\u00f5\u0000\u0000\u0a60\u0a84\u0005"+
		"F\u0000\u0000\u0a61\u0a62\u0005\u0117\u0000\u0000\u0a62\u0a63\u0005\u00a7"+
		"\u0000\u0000\u0a63\u0a66\u0005\u0096\u0000\u0000\u0a64\u0a65\u0005\u0019"+
		"\u0000\u0000\u0a65\u0a67\u0003t:\u0000\u0a66\u0a64\u0001\u0000\u0000\u0000"+
		"\u0a66\u0a67\u0001\u0000\u0000\u0000\u0a67\u0a68\u0001\u0000\u0000\u0000"+
		"\u0a68\u0a69\u0005\u00f5\u0000\u0000\u0a69\u0a75\u0005v\u0000\u0000\u0a6a"+
		"\u0a6b\u0005\u0002\u0000\u0000\u0a6b\u0a70\u0003\u00dam\u0000\u0a6c\u0a6d"+
		"\u0005\u0004\u0000\u0000\u0a6d\u0a6f\u0003\u00dam\u0000\u0a6e\u0a6c\u0001"+
		"\u0000\u0000\u0000\u0a6f\u0a72\u0001\u0000\u0000\u0000\u0a70\u0a6e\u0001"+
		"\u0000\u0000\u0000\u0a70\u0a71\u0001\u0000\u0000\u0000\u0a71\u0a73\u0001"+
		"\u0000\u0000\u0000\u0a72\u0a70\u0001\u0000\u0000\u0000\u0a73\u0a74\u0005"+
		"\u0003\u0000\u0000\u0a74\u0a76\u0001\u0000\u0000\u0000\u0a75\u0a6a\u0001"+
		"\u0000\u0000\u0000\u0a75\u0a76\u0001\u0000\u0000\u0000\u0a76\u0a77\u0001"+
		"\u0000\u0000\u0000\u0a77\u0a78\u0005\u0113\u0000\u0000\u0a78\u0a79\u0005"+
		"\u0002\u0000\u0000\u0a79\u0a7e\u0003t:\u0000\u0a7a\u0a7b\u0005\u0004\u0000"+
		"\u0000\u0a7b\u0a7d\u0003t:\u0000\u0a7c\u0a7a\u0001\u0000\u0000\u0000\u0a7d"+
		"\u0a80\u0001\u0000\u0000\u0000\u0a7e\u0a7c\u0001\u0000\u0000\u0000\u0a7e"+
		"\u0a7f\u0001\u0000\u0000\u0000\u0a7f\u0a81\u0001\u0000\u0000\u0000\u0a80"+
		"\u0a7e\u0001\u0000\u0000\u0000\u0a81\u0a82\u0005\u0003\u0000\u0000\u0a82"+
		"\u0a84\u0001\u0000\u0000\u0000\u0a83\u0a43\u0001\u0000\u0000\u0000\u0a83"+
		"\u0a59\u0001\u0000\u0000\u0000\u0a83\u0a61\u0001\u0000\u0000\u0000\u0a84"+
		"\u00af\u0001\u0000\u0000\u0000\u0a85\u0a8b\u0005\u00b8\u0000\u0000\u0a86"+
		"\u0a8c\u0003\u00dam\u0000\u0a87\u0a88\u0005\u0002\u0000\u0000\u0a88\u0a89"+
		"\u0003:\u001d\u0000\u0a89\u0a8a\u0005\u0003\u0000\u0000\u0a8a\u0a8c\u0001"+
		"\u0000\u0000\u0000\u0a8b\u0a86\u0001\u0000\u0000\u0000\u0a8b\u0a87\u0001"+
		"\u0000\u0000\u0000\u0a8c\u00b1\u0001\u0000\u0000\u0000\u0a8d\u0a8e\u0005"+
		"\u009a\u0000\u0000\u0a8e\u0a93\u0003T*\u0000\u0a8f\u0a90\u0005\u0004\u0000"+
		"\u0000\u0a90\u0a92\u0003T*\u0000\u0a91\u0a8f\u0001\u0000\u0000\u0000\u0a92"+
		"\u0a95\u0001\u0000\u0000\u0000\u0a93\u0a91\u0001\u0000\u0000\u0000\u0a93"+
		"\u0a94\u0001\u0000\u0000\u0000\u0a94\u0a97\u0001\u0000\u0000\u0000\u0a95"+
		"\u0a93\u0001\u0000\u0000\u0000\u0a96\u0a8d\u0001\u0000\u0000\u0000\u0a96"+
		"\u0a97\u0001\u0000\u0000\u0000\u0a97\u0a98\u0001\u0000\u0000\u0000\u0a98"+
		"\u0a9c\u0003\u00b4Z\u0000\u0a99\u0a9a\u0005\u0015\u0000\u0000\u0a9a\u0a9b"+
		"\u0005\u0095\u0000\u0000\u0a9b\u0a9d\u0003Z-\u0000\u0a9c\u0a99\u0001\u0000"+
		"\u0000\u0000\u0a9c\u0a9d\u0001\u0000\u0000\u0000\u0a9d\u0a9f\u0001\u0000"+
		"\u0000\u0000\u0a9e\u0aa0\u0007\u000e\u0000\u0000\u0a9f\u0a9e\u0001\u0000"+
		"\u0000\u0000\u0a9f\u0aa0\u0001\u0000\u0000\u0000\u0aa0\u0aa6\u0001\u0000"+
		"\u0000\u0000\u0aa1\u0aa2\u0005\u00bf\u0000\u0000\u0aa2\u0aa3\u0005\u0002"+
		"\u0000\u0000\u0aa3\u0aa4\u0003\u00b8\\\u0000\u0aa4\u0aa5\u0005\u0003\u0000"+
		"\u0000\u0aa5\u0aa7\u0001\u0000\u0000\u0000\u0aa6\u0aa1\u0001\u0000\u0000"+
		"\u0000\u0aa6\u0aa7\u0001\u0000\u0000\u0000\u0aa7\u0ab1\u0001\u0000\u0000"+
		"\u0000\u0aa8\u0aa9\u0005\u00ed\u0000\u0000\u0aa9\u0aae\u0003\\.\u0000"+
		"\u0aaa\u0aab\u0005\u0004\u0000\u0000\u0aab\u0aad\u0003\\.\u0000\u0aac"+
		"\u0aaa\u0001\u0000\u0000\u0000\u0aad\u0ab0\u0001\u0000\u0000\u0000\u0aae"+
		"\u0aac\u0001\u0000\u0000\u0000\u0aae\u0aaf\u0001\u0000\u0000\u0000\u0aaf"+
		"\u0ab2\u0001\u0000\u0000\u0000\u0ab0\u0aae\u0001\u0000\u0000\u0000\u0ab1"+
		"\u0aa8\u0001\u0000\u0000\u0000\u0ab1\u0ab2\u0001\u0000\u0000\u0000\u0ab2"+
		"\u0abc\u0001\u0000\u0000\u0000\u0ab3\u0ab4\u0005D\u0000\u0000\u0ab4\u0ab9"+
		"\u0003^/\u0000\u0ab5\u0ab6\u0005\u0004\u0000\u0000\u0ab6\u0ab8\u0003^"+
		"/\u0000\u0ab7\u0ab5\u0001\u0000\u0000\u0000\u0ab8\u0abb\u0001\u0000\u0000"+
		"\u0000\u0ab9\u0ab7\u0001\u0000\u0000\u0000\u0ab9\u0aba\u0001\u0000\u0000"+
		"\u0000\u0aba\u0abd\u0001\u0000\u0000\u0000\u0abb\u0ab9\u0001\u0000\u0000"+
		"\u0000\u0abc\u0ab3\u0001\u0000\u0000\u0000\u0abc\u0abd\u0001\u0000\u0000"+
		"\u0000\u0abd\u00b3\u0001\u0000\u0000\u0000\u0abe\u0abf\u0005\u00ca\u0000"+
		"\u0000\u0abf\u0ad7\u0003\u00b6[\u0000\u0ac0\u0ac1\u0005\u00dc\u0000\u0000"+
		"\u0ac1\u0ad7\u0003\u00b6[\u0000\u0ac2\u0ac3\u0005l\u0000\u0000\u0ac3\u0ad7"+
		"\u0003\u00b6[\u0000\u0ac4\u0ac5\u0005\u00ca\u0000\u0000\u0ac5\u0ac6\u0005"+
		"!\u0000\u0000\u0ac6\u0ac7\u0003\u00b6[\u0000\u0ac7\u0ac8\u0005\u0019\u0000"+
		"\u0000\u0ac8\u0ac9\u0003\u00b6[\u0000\u0ac9\u0ad7\u0001\u0000\u0000\u0000"+
		"\u0aca\u0acb\u0005\u00dc\u0000\u0000\u0acb\u0acc\u0005!\u0000\u0000\u0acc"+
		"\u0acd\u0003\u00b6[\u0000\u0acd\u0ace\u0005\u0019\u0000\u0000\u0ace\u0acf"+
		"\u0003\u00b6[\u0000\u0acf\u0ad7\u0001\u0000\u0000\u0000\u0ad0\u0ad1\u0005"+
		"l\u0000\u0000\u0ad1\u0ad2\u0005!\u0000\u0000\u0ad2\u0ad3\u0003\u00b6["+
		"\u0000\u0ad3\u0ad4\u0005\u0019\u0000\u0000\u0ad4\u0ad5\u0003\u00b6[\u0000"+
		"\u0ad5\u0ad7\u0001\u0000\u0000\u0000\u0ad6\u0abe\u0001\u0000\u0000\u0000"+
		"\u0ad6\u0ac0\u0001\u0000\u0000\u0000\u0ad6\u0ac2\u0001\u0000\u0000\u0000"+
		"\u0ad6\u0ac4\u0001\u0000\u0000\u0000\u0ad6\u0aca\u0001\u0000\u0000\u0000"+
		"\u0ad6\u0ad0\u0001\u0000\u0000\u0000\u0ad7\u00b5\u0001\u0000\u0000\u0000"+
		"\u0ad8\u0ad9\u0005\u0102\u0000\u0000\u0ad9\u0ae2\u0005\u00c3\u0000\u0000"+
		"\u0ada\u0adb\u0005\u0102\u0000\u0000\u0adb\u0ae2\u0005`\u0000\u0000\u0adc"+
		"\u0add\u00056\u0000\u0000\u0add\u0ae2\u0005\u00db\u0000\u0000\u0ade\u0adf"+
		"\u0003t:\u0000\u0adf\u0ae0\u0007\u001b\u0000\u0000\u0ae0\u0ae2\u0001\u0000"+
		"\u0000\u0000\u0ae1\u0ad8\u0001\u0000\u0000\u0000\u0ae1\u0ada\u0001\u0000"+
		"\u0000\u0000\u0ae1\u0adc\u0001\u0000\u0000\u0000\u0ae1\u0ade\u0001\u0000"+
		"\u0000\u0000\u0ae2\u00b7\u0001\u0000\u0000\u0000\u0ae3\u0ae4\u0006\\\uffff"+
		"\uffff\u0000\u0ae4\u0ae6\u0003\u00ba]\u0000\u0ae5\u0ae7\u0003\u00bc^\u0000"+
		"\u0ae6\u0ae5\u0001\u0000\u0000\u0000\u0ae6\u0ae7\u0001\u0000\u0000\u0000"+
		"\u0ae7\u0aef\u0001\u0000\u0000\u0000\u0ae8\u0ae9\n\u0002\u0000\u0000\u0ae9"+
		"\u0aee\u0003\u00b8\\\u0003\u0aea\u0aeb\n\u0001\u0000\u0000\u0aeb\u0aec"+
		"\u0005\u000b\u0000\u0000\u0aec\u0aee\u0003\u00b8\\\u0002\u0aed\u0ae8\u0001"+
		"\u0000\u0000\u0000\u0aed\u0aea\u0001\u0000\u0000\u0000\u0aee\u0af1\u0001"+
		"\u0000\u0000\u0000\u0aef\u0aed\u0001\u0000\u0000\u0000\u0aef\u0af0\u0001"+
		"\u0000\u0000\u0000\u0af0\u00b9\u0001\u0000\u0000\u0000\u0af1\u0aef\u0001"+
		"\u0000\u0000\u0000\u0af2\u0b0c\u0003\u00dam\u0000\u0af3\u0af4\u0005\u0002"+
		"\u0000\u0000\u0af4\u0b0c\u0005\u0003\u0000\u0000\u0af5\u0af6\u0005\u00c1"+
		"\u0000\u0000\u0af6\u0af7\u0005\u0002\u0000\u0000\u0af7\u0afc\u0003\u00b8"+
		"\\\u0000\u0af8\u0af9\u0005\u0004\u0000\u0000\u0af9\u0afb\u0003\u00b8\\"+
		"\u0000\u0afa\u0af8\u0001\u0000\u0000\u0000\u0afb\u0afe\u0001\u0000\u0000"+
		"\u0000\u0afc\u0afa\u0001\u0000\u0000\u0000\u0afc\u0afd\u0001\u0000\u0000"+
		"\u0000\u0afd\u0aff\u0001\u0000\u0000\u0000\u0afe\u0afc\u0001\u0000\u0000"+
		"\u0000\u0aff\u0b00\u0005\u0003\u0000\u0000\u0b00\u0b0c\u0001\u0000\u0000"+
		"\u0000\u0b01\u0b02\u0005\u0002\u0000\u0000\u0b02\u0b03\u0003\u00b8\\\u0000"+
		"\u0b03\u0b04\u0005\u0003\u0000\u0000\u0b04\u0b0c\u0001\u0000\u0000\u0000"+
		"\u0b05\u0b0c\u0005\f\u0000\u0000\u0b06\u0b0c\u0005\r\u0000\u0000\u0b07"+
		"\u0b08\u0005\u000e\u0000\u0000\u0b08\u0b09\u0003\u00b8\\\u0000\u0b09\u0b0a"+
		"\u0005\u000f\u0000\u0000\u0b0a\u0b0c\u0001\u0000\u0000\u0000\u0b0b\u0af2"+
		"\u0001\u0000\u0000\u0000\u0b0b\u0af3\u0001\u0000\u0000\u0000\u0b0b\u0af5"+
		"\u0001\u0000\u0000\u0000\u0b0b\u0b01\u0001\u0000\u0000\u0000\u0b0b\u0b05"+
		"\u0001\u0000\u0000\u0000\u0b0b\u0b06\u0001\u0000\u0000\u0000\u0b0b\u0b07"+
		"\u0001\u0000\u0000\u0000\u0b0c\u00bb\u0001\u0000\u0000\u0000\u0b0d\u0b0f"+
		"\u0005\u012a\u0000\u0000\u0b0e\u0b10\u0005\u012e\u0000\u0000\u0b0f\u0b0e"+
		"\u0001\u0000\u0000\u0000\u0b0f\u0b10\u0001\u0000\u0000\u0000\u0b10\u0b2c"+
		"\u0001\u0000\u0000\u0000\u0b11\u0b13\u0005\u0128\u0000\u0000\u0b12\u0b14"+
		"\u0005\u012e\u0000\u0000\u0b13\u0b12\u0001\u0000\u0000\u0000\u0b13\u0b14"+
		"\u0001\u0000\u0000\u0000\u0b14\u0b2c\u0001\u0000\u0000\u0000\u0b15\u0b17"+
		"\u0005\u012e\u0000\u0000\u0b16\u0b18\u0005\u012e\u0000\u0000\u0b17\u0b16"+
		"\u0001\u0000\u0000\u0000\u0b17\u0b18\u0001\u0000\u0000\u0000\u0b18\u0b2c"+
		"\u0001\u0000\u0000\u0000\u0b19\u0b1a\u0005\u0010\u0000\u0000\u0b1a\u0b1b"+
		"\u0005\u0132\u0000\u0000\u0b1b\u0b1d\u0005\u0011\u0000\u0000\u0b1c\u0b1e"+
		"\u0005\u012e\u0000\u0000\u0b1d\u0b1c\u0001\u0000\u0000\u0000\u0b1d\u0b1e"+
		"\u0001\u0000\u0000\u0000\u0b1e\u0b2c\u0001\u0000\u0000\u0000\u0b1f\u0b21"+
		"\u0005\u0010\u0000\u0000\u0b20\u0b22\u0005\u0132\u0000\u0000\u0b21\u0b20"+
		"\u0001\u0000\u0000\u0000\u0b21\u0b22\u0001\u0000\u0000\u0000\u0b22\u0b23"+
		"\u0001\u0000\u0000\u0000\u0b23\u0b25\u0005\u0004\u0000\u0000\u0b24\u0b26"+
		"\u0005\u0132\u0000\u0000\u0b25\u0b24\u0001\u0000\u0000\u0000\u0b25\u0b26"+
		"\u0001\u0000\u0000\u0000\u0b26\u0b27\u0001\u0000\u0000\u0000\u0b27\u0b29"+
		"\u0005\u0011\u0000\u0000\u0b28\u0b2a\u0005\u012e\u0000\u0000\u0b29\u0b28"+
		"\u0001\u0000\u0000\u0000\u0b29\u0b2a\u0001\u0000\u0000\u0000\u0b2a\u0b2c"+
		"\u0001\u0000\u0000\u0000\u0b2b\u0b0d\u0001\u0000\u0000\u0000\u0b2b\u0b11"+
		"\u0001\u0000\u0000\u0000\u0b2b\u0b15\u0001\u0000\u0000\u0000\u0b2b\u0b19"+
		"\u0001\u0000\u0000\u0000\u0b2b\u0b1f\u0001\u0000\u0000\u0000\u0b2c\u00bd"+
		"\u0001\u0000\u0000\u0000\u0b2d\u0b2e\u0003\u00dam\u0000\u0b2e\u0b2f\u0005"+
		"\u0122\u0000\u0000\u0b2f\u0b30\u0003t:\u0000\u0b30\u00bf\u0001\u0000\u0000"+
		"\u0000\u0b31\u0b32\u0005b\u0000\u0000\u0b32\u0b36\u0007\u001c\u0000\u0000"+
		"\u0b33\u0b34\u0005\u0100\u0000\u0000\u0b34\u0b36\u0007\u001d\u0000\u0000"+
		"\u0b35\u0b31\u0001\u0000\u0000\u0000\u0b35\u0b33\u0001\u0000\u0000\u0000"+
		"\u0b36\u00c1\u0001\u0000\u0000\u0000\u0b37\u0b38\u0005}\u0000\u0000\u0b38"+
		"\u0b39\u0005\u008c\u0000\u0000\u0b39\u0b3d\u0003\u00c4b\u0000\u0b3a\u0b3b"+
		"\u0005\u00cb\u0000\u0000\u0b3b\u0b3d\u0007\u001e\u0000\u0000\u0b3c\u0b37"+
		"\u0001\u0000\u0000\u0000\u0b3c\u0b3a\u0001\u0000\u0000\u0000\u0b3d\u00c3"+
		"\u0001\u0000\u0000\u0000\u0b3e\u0b3f\u0005\u00cb\u0000\u0000\u0b3f\u0b46"+
		"\u0005\u0103\u0000\u0000\u0b40\u0b41\u0005\u00cb\u0000\u0000\u0b41\u0b46"+
		"\u0005.\u0000\u0000\u0b42\u0b43\u0005\u00cf\u0000\u0000\u0b43\u0b46\u0005"+
		"\u00cb\u0000\u0000\u0b44\u0b46\u0005\u00e5\u0000\u0000\u0b45\u0b3e\u0001"+
		"\u0000\u0000\u0000\u0b45\u0b40\u0001\u0000\u0000\u0000\u0b45\u0b42\u0001"+
		"\u0000\u0000\u0000\u0b45\u0b44\u0001\u0000\u0000\u0000\u0b46\u00c5\u0001"+
		"\u0000\u0000\u0000\u0b47\u0b4d\u0003t:\u0000\u0b48\u0b49\u0003\u00dam"+
		"\u0000\u0b49\u0b4a\u0005\u0006\u0000\u0000\u0b4a\u0b4b\u0003t:\u0000\u0b4b"+
		"\u0b4d\u0001\u0000\u0000\u0000\u0b4c\u0b47\u0001\u0000\u0000\u0000\u0b4c"+
		"\u0b48\u0001\u0000\u0000\u0000\u0b4d\u00c7\u0001\u0000\u0000\u0000\u0b4e"+
		"\u0b4f\u0003\u00dam\u0000\u0b4f\u0b50\u0005\u0001\u0000\u0000\u0b50\u0b51"+
		"\u0003\u00dam\u0000\u0b51\u0b54\u0001\u0000\u0000\u0000\u0b52\u0b54\u0003"+
		"\u00dam\u0000\u0b53\u0b4e\u0001\u0000\u0000\u0000\u0b53\u0b52\u0001\u0000"+
		"\u0000\u0000\u0b54\u00c9\u0001\u0000\u0000\u0000\u0b55\u0b5a\u0003\u00c8"+
		"d\u0000\u0b56\u0b57\u0005\u0004\u0000\u0000\u0b57\u0b59\u0003\u00c8d\u0000"+
		"\u0b58\u0b56\u0001\u0000\u0000\u0000\u0b59\u0b5c\u0001\u0000\u0000\u0000"+
		"\u0b5a\u0b58\u0001\u0000\u0000\u0000\u0b5a\u0b5b\u0001\u0000\u0000\u0000"+
		"\u0b5b\u00cb\u0001\u0000\u0000\u0000\u0b5c\u0b5a\u0001\u0000\u0000\u0000"+
		"\u0b5d\u0b5e\u0007\u001f\u0000\u0000\u0b5e\u00cd\u0001\u0000\u0000\u0000"+
		"\u0b5f\u0b64\u0003\u00dam\u0000\u0b60\u0b61\u0005\u0001\u0000\u0000\u0b61"+
		"\u0b63\u0003\u00dam\u0000\u0b62\u0b60\u0001\u0000\u0000\u0000\u0b63\u0b66"+
		"\u0001\u0000\u0000\u0000\u0b64\u0b62\u0001\u0000\u0000\u0000\u0b64\u0b65"+
		"\u0001\u0000\u0000\u0000\u0b65\u00cf\u0001\u0000\u0000\u0000\u0b66\u0b64"+
		"\u0001\u0000\u0000\u0000\u0b67\u0b68\u0005a\u0000\u0000\u0b68\u0b69\u0003"+
		"\u00d2i\u0000\u0b69\u0b6a\u0005\u001c\u0000\u0000\u0b6a\u0b6b\u0005\u00ac"+
		"\u0000\u0000\u0b6b\u0b6c\u0003z=\u0000\u0b6c\u00d1\u0001\u0000\u0000\u0000"+
		"\u0b6d\u0b6e\u0007 \u0000\u0000\u0b6e\u00d3\u0001\u0000\u0000\u0000\u0b6f"+
		"\u0b73\u0003\u00d6k\u0000\u0b70\u0b73\u0005>\u0000\u0000\u0b71\u0b73\u0005"+
		":\u0000\u0000\u0b72\u0b6f\u0001\u0000\u0000\u0000\u0b72\u0b70\u0001\u0000"+
		"\u0000\u0000\u0b72\u0b71\u0001\u0000\u0000\u0000\u0b73\u00d5\u0001\u0000"+
		"\u0000\u0000\u0b74\u0b7a\u0003\u00dam\u0000\u0b75\u0b76\u0005\u010c\u0000"+
		"\u0000\u0b76\u0b7a\u0003\u00dam\u0000\u0b77\u0b78\u0005\u00d7\u0000\u0000"+
		"\u0b78\u0b7a\u0003\u00dam\u0000\u0b79\u0b74\u0001\u0000\u0000\u0000\u0b79"+
		"\u0b75\u0001\u0000\u0000\u0000\u0b79\u0b77\u0001\u0000\u0000\u0000\u0b7a"+
		"\u00d7\u0001\u0000\u0000\u0000\u0b7b\u0b80\u0003\u00dam\u0000\u0b7c\u0b7d"+
		"\u0005\u0004\u0000\u0000\u0b7d\u0b7f\u0003\u00dam\u0000\u0b7e\u0b7c\u0001"+
		"\u0000\u0000\u0000\u0b7f\u0b82\u0001\u0000\u0000\u0000\u0b80\u0b7e\u0001"+
		"\u0000\u0000\u0000\u0b80\u0b81\u0001\u0000\u0000\u0000\u0b81\u00d9\u0001"+
		"\u0000\u0000\u0000\u0b82\u0b80\u0001\u0000\u0000\u0000\u0b83\u0b89\u0005"+
		"\u0135\u0000\u0000\u0b84\u0b89\u0005\u0137\u0000\u0000\u0b85\u0b89\u0003"+
		"\u00deo\u0000\u0b86\u0b89\u0005\u0138\u0000\u0000\u0b87\u0b89\u0005\u0136"+
		"\u0000\u0000\u0b88\u0b83\u0001\u0000\u0000\u0000\u0b88\u0b84\u0001\u0000"+
		"\u0000\u0000\u0b88\u0b85\u0001\u0000\u0000\u0000\u0b88\u0b86\u0001\u0000"+
		"\u0000\u0000\u0b88\u0b87\u0001\u0000\u0000\u0000\u0b89\u00db\u0001\u0000"+
		"\u0000\u0000\u0b8a\u0b8c\u0005\u0129\u0000\u0000\u0b8b\u0b8a\u0001\u0000"+
		"\u0000\u0000\u0b8b\u0b8c\u0001\u0000\u0000\u0000\u0b8c\u0b8d\u0001\u0000"+
		"\u0000\u0000\u0b8d\u0b97\u0005\u0133\u0000\u0000\u0b8e\u0b90\u0005\u0129"+
		"\u0000\u0000\u0b8f\u0b8e\u0001\u0000\u0000\u0000\u0b8f\u0b90\u0001\u0000"+
		"\u0000\u0000\u0b90\u0b91\u0001\u0000\u0000\u0000\u0b91\u0b97\u0005\u0134"+
		"\u0000\u0000\u0b92\u0b94\u0005\u0129\u0000\u0000\u0b93\u0b92\u0001\u0000"+
		"\u0000\u0000\u0b93\u0b94\u0001\u0000\u0000\u0000\u0b94\u0b95\u0001\u0000"+
		"\u0000\u0000\u0b95\u0b97\u0005\u0132\u0000\u0000\u0b96\u0b8b\u0001\u0000"+
		"\u0000\u0000\u0b96\u0b8f\u0001\u0000\u0000\u0000\u0b96\u0b93\u0001\u0000"+
		"\u0000\u0000\u0b97\u00dd\u0001\u0000\u0000\u0000\u0b98\u0b99\u0007!\u0000"+
		"\u0000\u0b99\u00df\u0001\u0000\u0000\u0000\u018c\u00fc\u0101\u0105\u010c"+
		"\u0111\u0115\u011b\u011f\u0134\u0138\u013c\u0140\u0148\u014c\u014f\u0156"+
		"\u015f\u0165\u0169\u016f\u0176\u017f\u018b\u0194\u019d\u01a3\u01ae\u01b6"+
		"\u01be\u01c5\u01cf\u01d6\u01f3\u01f6\u01f9\u01fd\u0203\u0208\u020f\u0214"+
		"\u0218\u0220\u0226\u022a\u0238\u0240\u0253\u026c\u026f\u0279\u027d\u0284"+
		"\u028e\u0294\u0299\u029d\u02a3\u02ac\u02b2\u02b6\u02bd\u02c1\u02c9\u02ce"+
		"\u02d2\u02da\u02e2\u02e7\u02eb\u02f5\u02fc\u0301\u0305\u030f\u0312\u031b"+
		"\u0320\u0326\u033e\u0344\u0346\u034c\u0352\u0354\u035c\u035e\u0364\u036a"+
		"\u036c\u037b\u0380\u0387\u0393\u0395\u039d\u039f\u03b1\u03b4\u03b8\u03bc"+
		"\u03ce\u03d1\u03e1\u03eb\u03f0\u03f6\u03f9\u0402\u0404\u0407\u040d\u0414"+
		"\u0419\u041f\u0423\u0427\u042d\u0438\u0441\u044c\u045b\u045e\u0463\u0465"+
		"\u046c\u0472\u0474\u0478\u0482\u0488\u048b\u048d\u0499\u04a0\u04a4\u04a8"+
		"\u04ac\u04b3\u04bc\u04bf\u04c3\u04c8\u04cc\u04d4\u04d7\u04da\u04e1\u04ec"+
		"\u04ef\u04f9\u04fc\u0507\u050c\u0514\u0517\u051b\u0524\u052d\u0530\u0539"+
		"\u053c\u053f\u0543\u054e\u0551\u0558\u055b\u056e\u0572\u0576\u057a\u057e"+
		"\u0582\u0584\u058f\u0594\u059d\u05a6\u05a9\u05af\u05bb\u05be\u05c7\u05ca"+
		"\u05d2\u05d5\u05d8\u05dd\u05e0\u05ec\u05ef\u05f7\u05fc\u0600\u0602\u0604"+
		"\u0613\u0615\u0620\u0635\u063f\u064a\u064e\u0650\u0658\u065f\u066c\u0672"+
		"\u0682\u068b\u068e\u0696\u0699\u06a0\u06a5\u06b0\u06b3\u06b7\u06b9\u06c1"+
		"\u06cb\u06d1\u06d3\u06da\u06de\u06e0\u06e7\u06eb\u06ed\u06ef\u06f8\u0703"+
		"\u0707\u0711\u071b\u071f\u0727\u0729\u0736\u073e\u0747\u074d\u0755\u075b"+
		"\u075f\u0764\u0769\u076f\u077d\u077f\u079d\u07a8\u07b0\u07b5\u07ba\u07c7"+
		"\u07cd\u07d4\u07d9\u07dc\u07df\u07e4\u07eb\u07ee\u07f7\u07fa\u07fe\u0801"+
		"\u0804\u0813\u0816\u0829\u082d\u0835\u0839\u0852\u0855\u085e\u0864\u086a"+
		"\u0870\u0879\u087c\u087f\u0892\u089b\u08b1\u08b4\u08be\u08c7\u08cd\u08d3"+
		"\u08de\u08e0\u08e5\u08ec\u08ee\u08f4\u08fa\u0905\u090e\u0913\u0918\u091a"+
		"\u091c\u0922\u0924\u092e\u0937\u0939\u093f\u0941\u0944\u094e\u0950\u095c"+
		"\u095f\u0964\u0969\u0975\u0979\u097d\u0980\u0982\u098a\u098d\u0997\u099f"+
		"\u09a5\u09a7\u09af\u09b9\u09bf\u09cd\u09d6\u09dd\u09e2\u09e9\u09f3\u09f8"+
		"\u09ff\u0a19\u0a1e\u0a20\u0a27\u0a2b\u0a32\u0a36\u0a47\u0a56\u0a5d\u0a66"+
		"\u0a70\u0a75\u0a7e\u0a83\u0a8b\u0a93\u0a96\u0a9c\u0a9f\u0aa6\u0aae\u0ab1"+
		"\u0ab9\u0abc\u0ad6\u0ae1\u0ae6\u0aed\u0aef\u0afc\u0b0b\u0b0f\u0b13\u0b17"+
		"\u0b1d\u0b21\u0b25\u0b29\u0b2b\u0b35\u0b3c\u0b45\u0b4c\u0b53\u0b5a\u0b64"+
		"\u0b72\u0b79\u0b80\u0b88\u0b8b\u0b8f\u0b93\u0b96";
	public static final String _serializedATN = Utils.join(
		new String[] {
			_serializedATNSegment0,
			_serializedATNSegment1
		},
		""
	);
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}