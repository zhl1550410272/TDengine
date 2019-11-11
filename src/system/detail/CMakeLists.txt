CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

IF ((TD_LINUX_64) OR (TD_LINUX_32 AND TD_ARM))
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/inc)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/client/inc)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/modules/http/inc)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/modules/monitor/inc)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/inc)
  INCLUDE_DIRECTORIES(${TD_ENTERPRISE_DIR}/src/util/cluster/inc)
  INCLUDE_DIRECTORIES(${TD_OS_DIR}/inc)
  INCLUDE_DIRECTORIES(inc)
  AUX_SOURCE_DIRECTORY(./src SRC)
  LIST(REMOVE_ITEM SRC ./src/vnodeFileUtil.c)
  LIST(REMOVE_ITEM SRC ./src/taosGrant.c)

  # oem begin
  ADD_EXECUTABLE(powerd ${SRC})

  TARGET_LINK_LIBRARIES(powerd taos_static trpc tutil sdb monitor pthread http)

  IF (TD_LITE)
    TARGET_LINK_LIBRARIES(powerd taosd_lite)
  ELSE ()
    TARGET_LINK_LIBRARIES(powerd taosd_cluster)
  ENDIF ()
  # oem end

  SET(PREPARE_ENV_CMD "prepare_env_cmd")
  SET(PREPARE_ENV_TARGET "prepare_env_target")
  ADD_CUSTOM_COMMAND(OUTPUT ${PREPARE_ENV_CMD}
    POST_BUILD 
    COMMAND echo "make test directory"
    # oem begin
    DEPENDS powerd
    # oem end
    COMMAND ${CMAKE_COMMAND} -E make_directory ${TD_TESTS_OUTPUT_DIR}/cfg/
    COMMAND ${CMAKE_COMMAND} -E make_directory ${TD_TESTS_OUTPUT_DIR}/log/
    COMMAND ${CMAKE_COMMAND} -E make_directory ${TD_TESTS_OUTPUT_DIR}/data/
    # oem begin
    COMMAND ${CMAKE_COMMAND} -E echo dataDir ${TD_TESTS_OUTPUT_DIR}/data > ${TD_TESTS_OUTPUT_DIR}/cfg/power.cfg
    COMMAND ${CMAKE_COMMAND} -E echo logDir ${TD_TESTS_OUTPUT_DIR}/log  >> ${TD_TESTS_OUTPUT_DIR}/cfg/power.cfg
    COMMAND ${CMAKE_COMMAND} -E echo charset UTF-8  >> ${TD_TESTS_OUTPUT_DIR}/cfg/power.cfg
    COMMENT "prepare powerd environment")
    # oem end
  ADD_CUSTOM_TARGET(${PREPARE_ENV_TARGET} ALL WORKING_DIRECTORY ${TD_EXECUTABLE_OUTPUT_PATH} DEPENDS ${PREPARE_ENV_CMD})

ENDIF ()
